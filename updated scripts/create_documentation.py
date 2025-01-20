import os
import ast

def get_functions_and_classes(filepath):
    """Extract all functions and classes from a Python file, along with docstrings."""
    functions = []
    classes = []
    
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            tree = ast.parse(file.read(), filename=filepath)
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    func_name = node.name
                    docstring = ast.get_docstring(node) or "No description available."
                    functions.append((func_name, docstring))
                elif isinstance(node, ast.ClassDef):
                    classes.append(node.name)
    except Exception as e:
        print(f"Error reading file {filepath}: {e}")
        
    return functions, classes

def scan_folder_for_python_files(folder_path):
    """Recursively scan folder and subfolders for Python scripts."""
    python_files = []
    
    # Walk through the directory, including subdirectories
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.py'):
                python_files.append(os.path.join(root, file))
    
    return python_files

def generate_documentation(folder_path, output_file="documentation.md"):
    """Generate documentation of all Python scripts in the folder."""
    python_files = scan_folder_for_python_files(folder_path)
    if not python_files:
        print(f"No Python files found in {folder_path}. Please check the folder path.")
        return
    
    with open(output_file, 'w', encoding='utf-8') as doc_file:
        doc_file.write("# Documentation of Python Scripts in Folder\n\n")
        
        for python_file in python_files:
            # Extract function and class names with docstrings
            functions, classes = get_functions_and_classes(python_file)
            
            doc_file.write(f"## `{os.path.basename(python_file)}`\n")
            
            # How to run the script (this can be customized)
            doc_file.write("### How to run this script:\n")
            doc_file.write(f"Run the script using the following command:\n")
            doc_file.write(f"```bash\npython {python_file}\n```\n")
            
            # Functions
            doc_file.write("- **Functions**:\n")
            if functions:
                for func, docstring in functions:
                    doc_file.write(f"  - `def {func}()`\n")
                    doc_file.write(f"    - Description: {docstring}\n")
            else:
                doc_file.write("  - None\n")
            
            # Classes
            doc_file.write("- **Classes**:\n")
            if classes:
                for cls in classes:
                    doc_file.write(f"  - `class {cls}`\n")
            else:
                doc_file.write("  - None\n")
            
            doc_file.write("\n")
    
    print(f"Documentation has been generated and saved to {output_file}")

def main():
    # Folder containing your Python scripts (you can specify a path or use the current directory)
    folder_path = os.path.dirname(os.path.abspath(__file__))  # Get the folder where the script is located
    
    # Generate documentation
    generate_documentation(folder_path)

if __name__ == "__main__":
    main()
