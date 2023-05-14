#!/bin/sh

cargo run -- --latest && cp ./target/debug/pricecatcher.zip pricecatcher.zip
git add ./ && git commit -m "daily update" && git push origin
