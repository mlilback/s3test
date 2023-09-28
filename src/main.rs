use std::process;
use aws_sdk_config::{config::Credentials};
use aws_sdk_s3::{Client, Config, Error};
use aws_sdk_s3::config::Region;
use aws_sdk_s3::types::BucketVersioningStatus::Enabled;
use clap::{Parser, Subcommand};

#[derive(Subcommand, Clone, Debug)]
enum Commands {
    ListFiles,
    ListVersions {
        name: String,
    }
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    const BUCKET_NAME: &str = "mlilback-test1";
    let creds = Credentials::from_keys("***REMOVED***", "***REMOVED***", None);
    let region= Region::new("us-east-1");
    let config = Config::builder()
        .credentials_provider(creds)
        .endpoint_url("https://nyc3.digitaloceanspaces.com")
        .region(region)
        .build();
    let client = Client::from_conf(config);

    //make sure versioning is enabled
    let v_res = client.get_bucket_versioning()
        .bucket(BUCKET_NAME)
        .send()
        .await?;
    if v_res.status.is_none() || *v_res.status().unwrap() != Enabled {
        println!("versioning not enabled");
        process::exit(1);
    }

    match &args.command {
        None => {
            println!("no command specified");
            process::exit(1);
        }
        Some(Commands::ListFiles) => {
            let result = client.list_objects_v2()
                .bucket(BUCKET_NAME)
                .send()
                .await?;
            if let Some(contents) = result.contents {
                for object in contents {
                    println!("Object: {:?}", object.key().unwrap_or("<none>"));
                }
            } else {
                println!("no contents");
            }
        }
        Some(Commands::ListVersions { name}) => {
            let ver_result = client.list_object_versions()
                .bucket(BUCKET_NAME)
                .set_prefix(Some(name.clone()))
                .send().await?;
            if let Some(versions) = ver_result.versions {
                for version in versions {
                    println!("version: {:?}: {}", version.version_id().unwrap(), version.size());
                }
            }
        }
    }
    Ok(())
}
