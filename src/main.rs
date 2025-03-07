use log::{debug, info};
use std::env;
use std::sync::mpsc;
use std::thread;
use std::thread::sleep;
use std::time::Duration;

use wal::common::WalPosition;
use wal::wal::Wal;

const NUM_TO_WRITE: usize = 20;

// This demonstrates how to use the wal. Open and begin recovery. Once it is recovered, then
fn main() {
    env_logger::init();
    let args: Vec<String> = env::args().collect();

    let uri = &args[1];
    println!("{}", uri);
    let uri = uri.parse().unwrap();
    let mut wal = Wal::open(uri).unwrap();

    for e in wal.iterate() {
        info!("Recovered {:?}", e.unwrap().0);
    }

    // Wrap in a mutex to share across the writing and completion threads.
    let (tx, rx) = mpsc::channel::<WalPosition>();

    thread::scope(|s| {
        // This thread will write data periodically. It represents the user thread.
        s.spawn(move || {
            // Make some dummy data.
            let mut data: [u8; 10000] = [0; 10000];
            for (i, pos) in data.iter_mut().enumerate() {
                *pos = i as u8;
            }
            let mut num_outstanding = 0;
            info!("Start writing");

            for i in 0..NUM_TO_WRITE {
                let loc = wal.append(&data).unwrap();
                info!("Wrote {i} at loc {loc:?}");
                num_outstanding += 1;
                num_outstanding -= notify_completions(&mut wal, &tx);
            }
            info!("Finished writing - waiting for {num_outstanding} lagging completion");

            while num_outstanding > 0 {
                sleep(Duration::from_millis(1));
                num_outstanding -= notify_completions(&mut wal, &tx);
            }
            info!("All synced to disk");
        });

        // This thread waits for data to be completed and returns it to the caller.
        s.spawn(move || {
            let mut num_outstanding = NUM_TO_WRITE;
            while num_outstanding > 0 {
                if rx.recv().is_ok() {
                    num_outstanding -= 1;
                }
            }
        });
    });
}

fn notify_completions(wal: &mut Wal, tx: &mpsc::Sender<WalPosition>) -> usize {
    let mut count = 0;
    for pos in wal.process_completions() {
        tx.send(pos).unwrap();
        debug!("Completion for {:?}", pos);
        count += 1;
    }
    count
}
