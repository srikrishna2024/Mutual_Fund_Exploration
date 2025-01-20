import os
import re

def convert_to_github_anchor_link(section_title):
    """
    Converts a section title to a GitHub-style anchor link.
    - Converts to lowercase
    - Replaces spaces with hyphens
    - Removes special characters (except hyphens)
    """
    # Convert to lowercase, replace spaces with hyphens, and remove non-alphanumeric characters
    section_title = section_title.strip().lower()
    section_title = re.sub(r'[^a-z0-9\s-]', '', section_title)  # remove special characters except spaces and hyphens
    section_title = re.sub(r'\s+', '-', section_title)  # replace spaces with hyphens
    return section_title

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
        if line.startswith("## "):  # Section headings like "## Subfolder 1"
            section_title = line.strip().lstrip("## ").strip()
            anchor_link = convert_to_github_anchor_link(section_title)
            toc_section += f"- [{section_title}](#{anchor_link})\n"

    # Add the Table of Contents to the README
    readme_content += toc_section + "\n"

    # Add the documentation content to the README after the table of contents
    readme_content += documentation_content

    # Create or overwrite the README.md file with the generated content
    with open(readme_file, 'w', encoding='utf-8') as readme:
        readme.write(readme_content)
    
    print(f"Successfully converted {doc_file} to {readme_file}")

if __name__ == "__main__":
    # Convert the documentation.md file to README.md
    convert_documentation_to_readme()
