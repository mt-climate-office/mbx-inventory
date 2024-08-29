# mbx-inventory
No Code Database for Managing Mesonet-in-a-Box Deployments and Inventory. This project uses [NocoDB](https://nocodb.com/), an open-source AirTable alternative. Use the following steps to get started:

1. `docker compose up --build -d`
2. Navigate to [0.0.0.0:8080](0.0.0.0:8080) and create a user.
3. Once logged on, click `Team & Settings` > `Tokens` > `Create new token`. 
4. Copy the token and run the following in the terminal: 
    - `echo "xc-token=paste_your_token_here" >> .env`
