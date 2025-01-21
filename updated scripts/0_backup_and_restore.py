import subprocess
import os

# PostgreSQL Database parameters
DB_PARAMS = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': 'admin123',
    'host': 'localhost',
    'port': '5432'
}

def create_backup():
    """Create a backup of the local PostgreSQL database."""
    print(f"Creating backup of {DB_PARAMS['dbname']}...")

    # Ask user for the folder path where the backup file should be saved
    backup_folder = input("Please enter the folder path where the backup file should be saved: ").strip()

    # Ensure the folder path is valid
    if not os.path.isdir(backup_folder):
        print("The specified folder path does not exist. Please try again.")
        return

    # Define the backup filename
    backup_file = os.path.join(backup_folder, 'database_backup.sql')

    # Full path to pg_dump (adjust this to your actual PostgreSQL installation path)
    pg_dump_path = r'C:\Program Files\PostgreSQL\17\bin\pg_dump'

    dump_command = [
        pg_dump_path,  # Use full path to pg_dump here
        '-h', DB_PARAMS['host'],
        '-p', DB_PARAMS['port'],
        '-U', DB_PARAMS['user'],
        '-F', 'c',  # Custom format
        '-f', backup_file,
        DB_PARAMS['dbname']
    ]

    # Set the environment variable for the PostgreSQL password
    env = os.environ.copy()
    env['PGPASSWORD'] = DB_PARAMS['password']

    try:
        subprocess.run(dump_command, env=env, check=True)
        print(f"Backup of {DB_PARAMS['dbname']} created successfully: {backup_file}")
    except subprocess.CalledProcessError as e:
        print(f"Error during backup: {e}")
        raise

def restore_backup():
    """Restore a backup to the local PostgreSQL database."""
    print("Restore Options:")
    print("1. Clean Restore (Drops existing objects and restores)")
    print("2. Fresh Database (Restores to a new database)")

    choice = input("Enter your choice (1 or 2): ").strip()

    if choice not in ['1', '2']:
        print("Invalid choice. Please select either 1 or 2.")
        return

    # Ask user for the source path of the backup file to restore
    backup_file = input("Please enter the full path of the backup file to restore: ").strip()

    # Ensure the file exists
    if not os.path.isfile(backup_file):
        print("The specified backup file does not exist. Please try again.")
        return

    # Full path to pg_restore (adjust this to your actual PostgreSQL installation path)
    pg_restore_path = r'C:\Program Files\PostgreSQL\17\bin\pg_restore'

    restore_command = [
        pg_restore_path,
        '--host', DB_PARAMS['host'],
        '--port', DB_PARAMS['port'],
        '--username', DB_PARAMS['user'],
        '--no-password',
        '--verbose',
        backup_file
    ]

    if choice == '1':  # Clean Restore
        restore_command.extend(['--dbname', DB_PARAMS['dbname'], '--clean'])
    elif choice == '2':  # Fresh Database
        new_db_name = input("Please enter the name of the new database: ").strip()

        # Create the new database
        create_db_command = [
            'createdb',
            '-h', DB_PARAMS['host'],
            '-p', DB_PARAMS['port'],
            '-U', DB_PARAMS['user'],
            new_db_name
        ]

        try:
            env = os.environ.copy()
            env['PGPASSWORD'] = DB_PARAMS['password']
            subprocess.run(create_db_command, env=env, check=True)
            print(f"New database '{new_db_name}' created successfully.")
        except subprocess.CalledProcessError as e:
            print(f"Error creating new database: {e}")
            return

        restore_command.extend(['--dbname', new_db_name])

    # Set the environment variable for the PostgreSQL password
    env = os.environ.copy()
    env['PGPASSWORD'] = DB_PARAMS['password']

    try:
        subprocess.run(restore_command, env=env, check=True)
        print("Backup restored successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error during restore: {e}")
        raise

def main():
    """Main function to prompt the user for backup or restore."""
    print("Choose an option:")
    print("1. Backup PostgreSQL Database")
    print("2. Restore PostgreSQL Database")

    choice = input("Enter your choice (1 or 2): ")

    try:
        if choice == '1':
            # Create a backup
            create_backup()
        elif choice == '2':
            # Restore the backup
            restore_backup()
        else:
            print("Invalid choice. Please select either 1 or 2.")
            return

    except Exception as e:
        print(f"An error occurred: {e}")
        exit(1)

if __name__ == '__main__':
    main()
