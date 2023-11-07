# s3test

A test client for S3 written in Rust. Provides functionality related to versioning for a project I was working on.

## Setup

Requires a `.env` file with the following variables:

```
ACCESS_KEY=<your access key>
SECRET_KEY=<your secret key>
BUCKET_NAME=<your bucket name>
REGION=<your region>
ENDPOINT=<your endpoint>
```

## Usage

```
$ cargo build
$ cargo run ls "" # list all objects in bucket
```
