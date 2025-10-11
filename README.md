# STADVDB_MCO1

## Overview
### MCO1
Project involves importing the IMDb dataset into a PostgreSQL RDS instance on AWS.

## Files
`import_imdb.sh`: Bash script to import the IMDb files into the PostgreSQL database

## Setup
NOTE: Some of these packages might not exist on the chosen EC2 instance adjust accordingly

1. **Ensure PostgreSQL client is installed** on the EC2 instance:
```bash
sudo yum install postgresql -y
```

2. **Upload the IMDb `.tsv` files** to the EC2 instance
```bash
scp -i your-key.pem *.tsv ec2-user@your-ec2-ip:/home/ec2-user/imports/
```
Ensure all required IMDb files are placed in `/home/ec2-user/imports/`

3. **Create the `.pgpass` file**
```bash
vim ~/.pgpass
```
Add the ff. line with the actual credentials:
```bash
<rds_endpoint>:<port>:<db_name>:<db_user>:<your_password>
```
Then run:
```bash
chmod 600 ~/.pgpass
```

4. **Run the `import_imdb.sh` script**
```bash
tmux new -s <session_name>
chmod +x import_imdb.sh
./import_imdb.sh
```
You can verify the row count of the `.tsv` file using the ff. command:
```bash
wc -l <filename>.tsv # Returns the row count + 1
```