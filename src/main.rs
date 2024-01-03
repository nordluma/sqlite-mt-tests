use anyhow::anyhow;
use clap::{Parser, Subcommand};
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use rusqlite::params;
use tokio_rusqlite::{Connection, Result};

const N_WORKERS: usize = 4;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Commands,
    #[arg(short, long)]
    workers: Option<usize>,
}

#[derive(Subcommand)]
enum Commands {
    Insert,
    Select,
    Delete,
}

#[derive(Clone)]
struct DB {
    conn: Connection,
}

impl DB {
    async fn new(file: &str) -> Result<Self> {
        Ok(Self {
            conn: Connection::open(file).await?,
        })
    }

    async fn create_table(&self) -> Result<()> {
        self.conn
            .call(|conn| {
                match conn.execute(
                    "CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY key,
                    name TEXT NOT NULL UNIQUE
                )",
                    (),
                ) {
                    Ok(tables_altered) => {
                        if tables_altered > 0 {
                            println!("Table created");
                        } else {
                            println!("Table exists");
                        }
                        Ok(())
                    }
                    Err(e) => Err(tokio_rusqlite::Error::Rusqlite(e)),
                }
            })
            .await
    }

    async fn insert<'a>(&self, user: User<'a>) -> Result<()> {
        let username = user.name.to_owned();
        self.conn
            .call(move |conn| {
                match conn.execute("INSERT INTO users (name) VALUES (?1)", params![username]) {
                    Ok(_) => Ok(()),
                    Err(e) => Err(tokio_rusqlite::Error::Rusqlite(e)),
                }
            })
            .await
    }

    async fn select_all_users(&self) -> Result<Vec<DbUser>> {
        let users = self
            .conn
            .call(|conn| {
                let mut stmt = conn.prepare("SELECT * FROM users")?;
                let rows = stmt
                    .query_map([], |row| Ok(DbUser::new(row.get(0)?, row.get(1)?)))?
                    .collect::<std::result::Result<Vec<DbUser>, rusqlite::Error>>()?;

                Ok(rows)
            })
            .await;

        users
    }

    async fn delete_all_users(&self) -> Result<()> {
        self.conn
            .call(|conn| match conn.execute("DELETE FROM users", ()) {
                Ok(n_rows) => {
                    println!("Deleted {} rows", n_rows);
                    Ok(())
                }
                Err(e) => Err(tokio_rusqlite::Error::Rusqlite(e)),
            })
            .await
    }
}

#[derive(Debug)]
struct DbUser {
    id: usize,
    name: String,
}

impl std::fmt::Display for DbUser {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "id: {} name: {}", self.id, self.name)
    }
}

impl DbUser {
    fn new(id: usize, name: String) -> Self {
        Self { id, name }
    }
}

#[derive(Clone, Copy, Debug)]
struct User<'a> {
    name: &'a str,
}

impl<'a> User<'a> {
    fn new(name: &str) -> User {
        User { name }
    }
}

fn create_users() -> Vec<&'static str> {
    (0..10_000)
        .map(|_| generate_name())
        .map(|s| -> &'static str { Box::leak(Box::new(s)) })
        .collect()
}

fn generate_name() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(15)
        .map(char::from)
        .collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Args::parse();

    let db = DB::new("test.db").await.map_err(|e| anyhow!(e))?;
    db.create_table().await?;

    let names = create_users();
    let users: Vec<User<'static>> = names
        .iter()
        .map(|n| -> &'static str { Box::leak(Box::new(n)) })
        .map(|n| User::new(n))
        .collect();

    match cli.command {
        Commands::Insert => {
            let workers = cli.workers.unwrap_or(N_WORKERS);
            run_insertion(db, users, workers).await?;
        }
        Commands::Select => {
            let users = db.select_all_users().await?;
            println!("{:#?}", users);
        }
        Commands::Delete => db.delete_all_users().await?,
    };

    Ok(())
}

async fn run_insertion(
    connection: DB,
    users: Vec<User<'static>>,
    n_workers: usize,
) -> anyhow::Result<()> {
    let mut handles = Vec::with_capacity(n_workers);

    let batch_users = (0..n_workers)
        .map(|offset| {
            users
                .iter()
                .cloned()
                .skip(offset)
                .step_by(n_workers)
                .collect()
        })
        .collect::<Vec<Vec<User>>>();

    for worker in 1..=n_workers {
        handles.push(tokio::task::spawn(batch_insertion(
            connection.clone(),
            format!("Worker: {}", worker),
            batch_users.get(worker - 1).unwrap().to_vec(),
        )))
    }

    for handle in handles {
        handle.await??;
    }

    Ok(())
}

async fn batch_insertion(
    connection: DB,
    worker_name: String,
    users: Vec<User<'_>>,
) -> anyhow::Result<()> {
    for user in users {
        if let Ok(_) = connection.insert(user).await {
            println!("{} inserted: {:?}", worker_name, user)
        }
    }

    Ok(())
}
