#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };

    use glitch::{Configuration, Simulator};
    use rayon::prelude::*;
    use tracing::Level;
    use tracing_subscriber::FmtSubscriber;

    use crate::{
        tests::{invariants::VsrInvariantChecker, state_machine::TestOp},
        Client, Replica, ReplicaConfig,
    };

    struct ClusterConfiguration {
        pub replica_count: usize,
        pub client_count: usize,
        pub requests_per_client: usize,
        pub request_retry: Duration,
    }

    impl Default for ClusterConfiguration {
        fn default() -> Self {
            ClusterConfiguration {
                replica_count: 3,
                client_count: 3,
                requests_per_client: 10,
                request_retry: Duration::from_millis(200),
            }
        }
    }

    fn run_single_simulation(
        mut config: Configuration,
        seed: u64,
        test_config: &ClusterConfiguration,
    ) -> Duration {
        config.seed = seed;
        config.max_sim_time = Duration::from_secs(1000);
        config.check_invariants_frequency = 2;

        let start_time = std::time::Instant::now();
        let replicas = (0..test_config.replica_count)
            .map(|replica_id| {
                Replica::new(
                    start_time,
                    test_config.replica_count,
                    replica_id,
                    ReplicaConfig::default(),
                )
            })
            .collect();

        let clients = (0..test_config.client_count)
            .map(|client_id| {
                let mut client = Client::new(
                    client_id,
                    test_config.replica_count,
                    test_config.request_retry,
                );
                for i in 1..=test_config.requests_per_client {
                    client.queue_request(TestOp {
                        client_id,
                        request_num: i,
                    });
                }
                client
            })
            .collect();

        let mut simulator =
            Simulator::new(start_time, replicas, clients, config, VsrInvariantChecker);

        let converged = simulator.run();
        assert!(converged, "failed on seed {}", seed);
        simulator.elapsed()
    }

    fn run_simulation(
        original_config: Configuration,
        test_config: ClusterConfiguration,
        total_seeds: usize,
        num_workers: usize,
    ) -> Duration {
        let progress = Arc::new(AtomicUsize::new(0));
        let start_time = std::time::Instant::now();

        (0..total_seeds)
            .into_par_iter()
            .with_max_len(total_seeds / num_workers)
            .map(|seed| {
                let current = progress.fetch_add(1, Ordering::Relaxed) + 1;
                if current % 1000 == 0 {
                    let elapsed = start_time.elapsed();
                    let seeds_per_sec = current as f64 / elapsed.as_secs_f64();
                    let remaining_seeds = total_seeds - current;
                    let estimated_remaining_secs = remaining_seeds as f64 / seeds_per_sec;

                    println!(
                        "Progress: {}% ({}/{}) | Elapsed: {:.1}m | Remaining: {:.1}m | Rate: {:.0} seeds/sec",
                        (current * 100) / total_seeds,
                        current,
                        total_seeds,
                        elapsed.as_secs_f64() / 60.0,
                        estimated_remaining_secs / 60.0,
                        seeds_per_sec
                    );
                }

                run_single_simulation(original_config.clone(), seed as u64, &test_config)
            })
            .reduce(|| Duration::from_secs(0), |acc, duration| acc + duration)
    }

    #[test]
    fn crashes_and_partitions() {
        let config = Configuration::default();

        let duration = run_simulation(config, ClusterConfiguration::default(), 1000000, 4);
        println!("total virtual duration: {:?}", duration);
    }

    #[test]
    fn larger_cluster() {
        let config = Configuration::default();
        let test_config = ClusterConfiguration {
            replica_count: 5,
            ..Default::default()
        };

        let duration = run_simulation(config, test_config, 10000, 4);
        println!("total virtual duration: {:?}", duration);
    }

    #[test]
    fn single_seed_with_tracing() {
        FmtSubscriber::builder()
            .with_max_level(Level::DEBUG)
            .pretty()
            .init();

        let config = Configuration::default();
        let test_config = ClusterConfiguration::default();
        let duration = run_single_simulation(config, 1, &test_config);
        println!("total virtual duration: {:?}", duration);
    }
}
