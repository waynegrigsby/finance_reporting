# Setup Environment

In order to properly run this program, the following must be established:

- Virtual Env
- New Environment Variables

### Step 1: 
Create .env file with the following contents: 
```.dotenv
export PYTHONUNBUFFERED=1
export PYTHONPATH={Path to virtualenv}/bin/python
```

### Step 2: 
Run `scripts/setup_env.zsh` or `source .env` in terminal. Either of these will establish step 1
variables within your local environment.


