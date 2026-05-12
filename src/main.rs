#![windows_subsystem = "windows"]

use std::error::Error;
use std::fs;
use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Mutex;

use bluest::{btuuid::bluetooth_uuid_from_u16, Adapter, Device, Uuid};
use futures_lite::stream::StreamExt;
use serde::Serialize;
use tokio::sync::watch::{self, Receiver, Sender};
use warp::Filter;

const HRS_UUID: Uuid = bluetooth_uuid_from_u16(0x180D);
const HRM_UUID: Uuid = bluetooth_uuid_from_u16(0x2A37);

#[tokio::main]
async fn main() {
    // Log file path: C:\Users\<user>\AppData\Local\miband-heart-rate\logs\
    let log_dir = std::env::var("LOCALAPPDATA")
        .map(|p| Path::new(&p).join("miband-heart-rate").join("logs"))
        .unwrap_or_else(|_| Path::new(env!("CARGO_MANIFEST_DIR")).join("logs"));
    fs::create_dir_all(&log_dir).expect("create log dir");
    let log_file = Mutex::new(fs::OpenOptions::new().create(true).append(true).open(log_dir.join("heart-rate.log")).expect("open log file"));

    let log = |line: &str| {
        let now = chrono::Local::now().format("%Y-%m-%dT%H:%M:%S%.6fZ");
        let mut f = log_file.lock().unwrap();
        writeln!(f, "[{now}] {line}").ok();
    };

    log("=== miband-heart-rate started ===");

    let (tx, rx) = watch::channel(HeartRate {
        value: 0,
        sensor_contact: None,
    });
    let result = tokio::join!(ble_scanner(tx, &log), web_server(rx, &log));
    log(&format!("=== miband-heart-rate exited: {result:?} ==="));
}

#[derive(Serialize)]
struct HeartRate {
    value: u16,
    sensor_contact: Option<bool>,
}

async fn web_server<F: Fn(&str)>(rx: Receiver<HeartRate>, log: &F) -> Result<(), Box<dyn Error>> {
    let root = warp::path::end().map(|| warp::reply::html(include_str!("../web/index.html")));
    let heartrate = warp::path!("heartrate").then(move || {
        let mut rx = rx.clone();
        async move {
            drop(rx.borrow_and_update());
            rx.changed().await.unwrap();
            warp::reply::json(&rx.borrow().value)
        }
    });

    let socket_addr: SocketAddr = ([127, 0, 0, 1], 3030).into();
    log(&format!("Start listening at http://{socket_addr:?}"));

    warp::serve(warp::get().and(root).or(heartrate))
        .run(socket_addr)
        .await;
    Err("Server stopped".into())
}

async fn ble_scanner<F: Fn(&str)>(tx: Sender<HeartRate>, log: &F) -> Result<(), Box<dyn Error>> {
    log("Initializing BLE adapter");
    let adapter = Adapter::default()
        .await
        .ok_or("Bluetooth adapter not found")?;
    log("BLE adapter ready");
    adapter.wait_available().await?;

    loop {
        // 1) Try already-connected devices with HRS service
        if let Ok(devices) = adapter.connected_devices_with_services(&[HRS_UUID]).await {
            if let Some(device) = devices.into_iter().next() {
                log(&format!("Found already-connected HRS device: {}", device.id()));
                if let Err(err) = handle_device(&adapter, &device, &tx, log).await {
                    log(&format!("Connection error: {err:?}"));
                }
                continue;
            }
        }

        // 2) Scan for devices with HRS service (15 second timeout)
        log("Starting scan");
        let mut scan = adapter.discover_devices(&[HRS_UUID]).await?;
        log("Scan started");
        let found = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            scan.next(),
        ).await;

        if let Ok(Some(Ok(device))) = found {
            log(&format!("Found Device: {} {:?}", device.id(), device.name_async().await));
            if let Err(err) = handle_device(&adapter, &device, &tx, log).await {
                log(&format!("Connection error: {err:?}"));
            }
            continue;
        }

        // 3) Nothing found, wait and retry
        log("Device not found, retrying in 5s...");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

async fn handle_device<F: Fn(&str)>(
    adapter: &Adapter,
    device: &Device,
    tx: &Sender<HeartRate>,
    log: &F,
) -> Result<(), Box<dyn Error>> {
    let device_id = device.id().to_string();

    // Connect
    if !device.is_connected().await {
        log(&format!("Connecting device: {}", device_id));
        adapter.connect_device(&device).await?;
        log(&format!("Connect done: {}", device_id));
    } else {
        log(&format!("Device already connected: {}", device_id));
    }

    // Discover services
    let heart_rate_services = device.discover_services_with_uuid(HRS_UUID).await?;
    let heart_rate_service = heart_rate_services
        .first()
        .ok_or("Device should has one heart rate service at least")?;

    // Discover characteristics
    let heart_rate_measurements = heart_rate_service
        .discover_characteristics_with_uuid(HRM_UUID)
        .await?;
    let heart_rate_measurement = heart_rate_measurements
        .first()
        .ok_or("HeartRateService should has one heart rate measurement characteristic at least")?;

    // Subscribe to notifications
    log(&format!("Subscribing to HRM notifications"));
    let mut updates = heart_rate_measurement.notify().await?;
    log("Notification stream established");

    let mut sample_count: u64 = 0;
    loop {
        match tokio::time::timeout(
            std::time::Duration::from_secs(30),
            updates.next(),
        ).await {
            Ok(Some(Ok(heart_rate))) => {
                sample_count += 1;
                let flag = *heart_rate.get(0).ok_or("No flag")?;

                // Heart Rate Value Format
                let mut heart_rate_value = *heart_rate.get(1).ok_or("No heart rate u8")? as u16;
                if flag & 0b00001 != 0 {
                    heart_rate_value |= (*heart_rate.get(2).ok_or("No heart rate u16")? as u16) << 8;
                }

                // Sensor Contact Supported
                let mut sensor_contact = None;
                if flag & 0b00100 != 0 {
                    sensor_contact = Some(flag & 0b00010 != 0)
                }
                log(&format!("HeartRateValue: {heart_rate_value}, SensorContactDetected: {sensor_contact:?}, count: {sample_count}"));
                if let Err(e) = tx.send(HeartRate {
                    value: heart_rate_value,
                    sensor_contact,
                }) {
                    log(&format!("Failed to send heart rate to watch channel: {e:?}"));
                }
            }
            Ok(Some(Err(e))) => {
                log(&format!("Notification error: {e:?}"));
                break;
            }
            Ok(None) => {
                log(&format!("Notification stream ended, total samples: {sample_count}"));
                break;
            }
            Err(_) => {
                log(&format!("No notification for 30s, assuming device disconnected (total samples: {sample_count})"));
                break;
            }
        }
    }

    Err("No longer heart rate notify".into())
}
