use std::sync::Arc;
use std::time::Duration;

use rand::distributions::uniform::{UniformInt, UniformSampler};
use rand::Rng;
use rand_distr::Distribution;
use tokio::time::MissedTickBehavior;

const RW_INT: Duration = Duration::from_secs(1);
const NUM_WRITERS: usize = 4;
const NUM_READERS: usize = 256;

async fn reader(client: reqwest::Client, list: Arc<Vec<String>>) {
    fuzzy_sleep().await;

    let mut interval = tokio::time::interval(RW_INT);

    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    let weights = list
        .iter()
        .map(|x| (x.bytes().next().unwrap() as f32).sqrt())
        .collect();

    let mut weighted = rand_distr::WeightedAliasIndex::new(weights).unwrap();

    loop {
        interval.tick().await;

        if let Err(e) = random_read(&client, &mut weighted).await {
            println!("Error: {:?}", e)
        }
    }
}

async fn appender(client: reqwest::Client) {
    fuzzy_sleep().await;

    let mut interval = tokio::time::interval(RW_INT);

    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        if let Err(e) = append(&client).await {
            println!("Error: {:?}", e)
        }
    }
}

async fn random_read(
    client: &reqwest::Client,
    index: &mut rand_distr::WeightedAliasIndex<f32>,
) -> reqwest::Result<()> {
    let files = list(client).await?;

    if let Some(key) = choose(&files, index) {
        read(client, key).await?;
    }

    Ok(())
}

async fn read(client: &reqwest::Client, key: &str) -> reqwest::Result<()> {
    let resp = client
        .get(format!("http://127.0.0.1:8080/{}", key))
        .send()
        .await?;

    let _payload = resp.bytes().await?;

    Ok(())
}

async fn list(client: &reqwest::Client) -> reqwest::Result<Vec<String>> {
    let resp = client.get("http://127.0.0.1:8080/").send().await?;

    let data = resp.json().await?;

    Ok(data)
}

async fn fuzzy_sleep() {
    let dist: UniformInt<u64> = rand::distributions::uniform::UniformInt::new(0, 1000);

    let ms: u64 = dist.sample(&mut rand::thread_rng());

    tokio::time::sleep(Duration::from_millis(ms)).await;
}

async fn append(client: &reqwest::Client) -> reqwest::Result<()> {
    let data = get_random();

    client
        .post("http://127.0.0.1:8080/")
        .body(data)
        .send()
        .await?;

    Ok(())
}

fn choose<'a>(
    v: &'a Vec<String>,
    index: &mut rand_distr::WeightedAliasIndex<f32>,
) -> Option<&'a str> {
    if v.is_empty() {
        None
    } else {
        Some(v[index.sample(&mut rand::thread_rng())].as_str())
    }
}

fn get_random() -> Vec<u8> {
    let mut data = vec![0; 1024 * 1024 * 64];

    let mut rng = rand::thread_rng();

    rng.fill(data.as_mut_slice());

    data
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let client = reqwest::Client::new();
    let list = Arc::new(list(&client).await.unwrap());

    for _ in 0..NUM_WRITERS {
        tokio::spawn(appender(client.clone()));
    }

    tokio::time::sleep(RW_INT).await;

    for _ in 0..NUM_READERS {
        tokio::spawn(reader(client.clone(), list.clone()));
    }

    let _: () = std::future::pending().await;
}
