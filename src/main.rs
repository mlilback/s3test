use std::error::Error;
use std::process;
use aws_sdk_config::{config::Credentials};
use aws_sdk_s3::{Client, Config};
use aws_sdk_s3::config::Region;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::BucketVersioningStatus::Enabled;
use aws_sdk_s3::types::ChecksumAlgorithm;
use clap::{Parser, Subcommand};
use md5::{Digest};

#[derive(Subcommand, Clone, Debug)]
enum Commands {
    ListFiles,
    Ls {
        prefix: String,
    },
    ListVersions {
        name: String,
    },
    PutVersion {
        name: String,
        file_path: String,
    },
    DeleteVersion {
        name: String,
        version: String,
    },
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[command(subcommand)]
    command: Option<Commands>,
}

const BUCKET_NAME: &str = "enlighten-server-local";

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
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
            display_object_list(result);
        }
        Some(Commands::Ls { prefix} ) => {
            let result = client.list_objects_v2()
                .bucket(BUCKET_NAME)
                .prefix(prefix.clone())
                .send()
                .await?;
            display_object_list(result);
        }
        Some(Commands::ListVersions { name}) => {
            let ver_result = client.list_object_versions()
                .bucket(BUCKET_NAME)
                .set_prefix(Some(name.clone()))
                .send().await?;
            if let Some(versions) = ver_result.versions {
                for version in versions {
                    let str = version.e_tag().unwrap().to_string().to_ascii_lowercase();
                    println!("version: {}: {} ({})", version.version_id().unwrap(), version.size(), &str[1..str.len()-1]);
                }
            }
        }
        Some(Commands::PutVersion { name, file_path }) => {
            let bytes = tokio::fs::read(file_path).await?;
            let hash = format!("{:x}", md5::Md5::digest(&bytes));
            let exist = get_version_for_hash(&client, &name, &hash).await?;
            if let Some(ver) = exist {
                println!("version already exists: {}", ver);
                process::exit(1);
            }
            let result = client.put_object()
                .bucket(BUCKET_NAME)
                .key(name)
                .checksum_algorithm(ChecksumAlgorithm::Sha256)
                .body(ByteStream::from(bytes))
                .send()
                .await?;
            println!("put version: {}", result.version_id().unwrap());
        }
        Some(Commands::DeleteVersion { name, version }) => {
            let result = client.delete_object()
                .bucket(BUCKET_NAME)
                .key(name)
                .version_id(version)
                .send()
                .await?;
            println!("delete result: {:?}", result);
        }
    }
    Ok(())
}

fn display_object_list(result: ListObjectsV2Output) {
    if let Some(contents) = result.contents {
        for object in contents {
            println!("Object: {:?}", object.key().unwrap_or("<none>"));
        }
    } else {
        println!("no contents");
    }
}

/// returns the version_id if already exists
async fn get_version_for_hash(client: &Client, name: &String, hash: &String) -> Result<Option<String>, Box<dyn Error>> {
    let ver_result = client.list_object_versions()
        .bucket(BUCKET_NAME)
        .set_prefix(Some(name.clone()))
        .send().await?;
    if let Some(versions) = ver_result.versions {
        for version in versions {
            let str = &version.e_tag().unwrap().to_string().to_ascii_lowercase();
            let str = str[1..str.len()-1].to_string();
            if str == *hash {
                return Ok(Some(version.version_id().unwrap().to_string()));
            }
        }
        return Ok(None)
    }
    Ok(None)
}
