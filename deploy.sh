#!/bin/bash

cd /var/www/html/data-insight
source /var/www/html/data-insight/venv/bin/activate

# Pull the latest changes
git pull origin dev

# Install dependencies
pip install -r requirements.txt

# Restart your application (adjust the command as needed)
sudo systemctl restart fastapi
