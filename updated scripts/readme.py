import os
import shutil

def convert_documentation_to_readme(doc_file="documentation.md", readme_file="README.md"):
    """Converts the documentation.md file into a README.md file."""
    if not os.path.exists(doc_file):
        print(f"{doc_file} does not exist. Please ensure the documentation.md file is present.")
        return
    
    # Read the content of the documentation.md file
    with open(doc_file, 'r', encoding='utf-8') as doc:
        documentation_content = doc.read()
    
    # Start the README.md content with an introductory section
    readme_content = """# Project Documentation

This repository contains Python scripts organized into various subfolders. This document provides a summary of the scripts, including descriptions of their functions, classes, and usage instructions based on the generated `documentation.md` file.

## Table of Contents

"""

    # Append the Table of Contents by extracting sections from documentation
    lines = documentation_content.splitlines()
    toc_section = ""
    section_found = False
    for line in lines:
        if line.startswith("## "):
            if section_found:
                toc_section += f"\n"
            toc_section += f"- [{line.strip()}](#{line.strip().lower().replace(' ', '-')})"
            section_found = True

    # Add the Table of Contents to the README
    readme_content += toc_section + "\n\n"

    # Add the documentation content to the README after the table of contents
    readme_content += documentation_content

    # Create or overwrite the README.md file with the generated content
    with open(readme_file, 'w', encoding='utf-8') as readme:
        readme.write(readme_content)
    
    print(f"Successfully converted {doc_file} to {readme_file}")

if __name__ == "__main__":
    # Convert the documentation.md file to README.md
    convert_documentation_to_readme()
