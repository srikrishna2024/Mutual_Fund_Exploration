import os
import ast
import subprocess
import sys

def get_imports_from_file(filepath):
    """Extract all imports from a Python file."""
    imports = set()
    
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            tree = ast.parse(file.read(), filename=filepath)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imports.add(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    imports.add(node.module)
    except Exception as e:
        print(f"Error reading file {filepath}: {e}")
        
    return imports

def scan_folder_for_python_files(folder_path):
    """Recursively scan folder and subfolders for Python scripts."""
    python_files = []
    
    # Walk through the directory, including subdirectories
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.py'):
                python_files.append(os.path.join(root, file))
    
    return python_files

def generate_requirements_txt(folder_path):
    """Generate a requirements.txt file for all Python dependencies."""
    all_imports = set()
    
    # Scan for Python scripts in the folder and subfolders
    python_files = scan_folder_for_python_files(folder_path)
    if not python_files:
        print(f"No Python files found in {folder_path}. Please check the folder path.")
        return
    
    print(f"Found {len(python_files)} Python files. Scanning for imports...")
    
    # Extract imports from each Python file
    for python_file in python_files:
        imports = get_imports_from_file(python_file)
        if imports:
            print(f"Found imports in {python_file}: {imports}")
        all_imports.update(imports)
    
    # Create a requirements.txt file with the list of unique imports
    if all_imports:
        with open('requirements.txt', 'w') as req_file:
            for imp in sorted(all_imports):
                req_file.write(f'{imp}\n')
        print(f"requirements.txt has been generated with the following dependencies:")
        for imp in sorted(all_imports):
            print(imp)
    else:
        print("No imports were found in any Python files.")

def install_requirements():
    """Install all packages listed in requirements.txt."""
    print("\nInstalling dependencies from requirements.txt...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("All dependencies have been successfully installed.")
    except subprocess.CalledProcessError as e:
        print(f"Error installing dependencies: {e}")

def main():
    # Replace this with the path to the folder containing your scripts
    folder_path = os.path.dirname(os.path.abspath(__file__))  # Get the folder where the script is located
    
    # Ask the user for their choice
    print("Choose an option:")
    print("1. Generate requirements.txt file")
    print("2. Generate requirements.txt file and install all packages")
    
    user_choice = input("Enter the number (1 or 2): ").strip()

    if user_choice == '1':
        generate_requirements_txt(folder_path)
    elif user_choice == '2':
        generate_requirements_txt(folder_path)
        install_requirements()
    else:
        print("Invalid option. Please choose either 1 or 2.")

if __name__ == "__main__":
    main()
