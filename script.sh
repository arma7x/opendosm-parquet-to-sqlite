#!/bin/sh
v=$(date)
cargo run -- --latest && cp ./target/debug/pricecatcher.zip pricecatcher.zip
git add ./ && git commit -m "daily update: $v" && git push origin
