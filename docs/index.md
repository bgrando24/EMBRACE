# EMBRACE - Documentation

## Dependencies

python version: 3.13

## Install and run locally

1. Clone repo
```sh
git clone https://github.com/bgrando24/EMBRACE.git
```

2. Create python virtual environment, activate it, and install project + dependencies
```sh
python3 -m venv venv        # or replace the last 'venv' with whatever name you want to use
source venv/bin/activate    # use "C:\> <venv>\Scripts\activate.bat" on windows
pip install -e . -r requirements.txt
```

3. Create and update environment variables
    * Note there are two `.env` files, one at the project root and one under scripts/mysql
```sh
# dont forget to fill in the placeholder values for both files
cp .env.example .env
cp scripts/mysql/.env.example scripts/mysql/.env  
```

4. Run locally
```sh
python3 src/main.py
```

</br>

## Building and running the Docker container

Build: `docker build -t <container_name> .`

Run: `docker run --env-file .env <container_name>`

---

</br></br>
