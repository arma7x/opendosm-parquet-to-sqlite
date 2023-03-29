#!/bin/sh

cargo run
cp ./target/debug/pricecatcher.zip pricecatcher.zip
git add ./ && git commit -m "daily update" && git push origin
