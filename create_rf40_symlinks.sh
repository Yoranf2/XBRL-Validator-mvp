#!/usr/bin/env bash
set -euo pipefail

# Create RF 4.0 taxonomy package symlinks
# This script creates relative symlinks from backend/github_work/taxonomies/eba/rf40/
# to the three RF 4.0 packages in github_work/eba-taxonomies/taxonomies/4.0/

ROOT="$(pwd)"
SRC_BASE="$ROOT/github_work/eba-taxonomies/taxonomies/4.0"
DEST_BASE="$ROOT/backend/github_work/taxonomies/eba/rf40"

echo "Creating RF 4.0 taxonomy package symlinks..."
echo "Source base: $SRC_BASE"
echo "Destination base: $DEST_BASE"

# Ensure destination directory exists
mkdir -p "$DEST_BASE"

# Define the mappings as arrays
NAMES=("rf-unpacked" "dict-unpacked" "severity-unpacked")
PATHS=(
  "EBA_XBRL_4.0_Reporting_Frameworks_4.0.0.0/EBA_XBRL_4.0_Reporting_Frameworks_4.0.0.0"
  "EBA_XBRL_4.0_Dictionary_4.0.0.0/EBA_XBRL_4.0_Dictionary_4.0.0.0"
  "EBA_XBRL_4.0_Severity_4.0.0.0/EBA_XBRL_4.0_Severity_4.0.0.0"
)

# Create symlinks
for i in "${!NAMES[@]}"; do
  name="${NAMES[$i]}"
  src="$SRC_BASE/${PATHS[$i]}"
  dest="$DEST_BASE/$name"
  
  echo "Processing $name..."
  
  # Check if source exists
  if [ ! -d "$src" ]; then
    echo "ERROR: Source directory does not exist: $src"
    exit 1
  fi
  
  # Remove existing symlink if it exists
  if [ -e "$dest" ] || [ -L "$dest" ]; then
    echo "  Removing existing symlink: $dest"
    rm -f "$dest"
  fi
  
  # Calculate relative path from dest to src
  rel_path=$(python3 - <<EOF
import os
import sys
src = sys.argv[1]
dest = sys.argv[2]
print(os.path.relpath(src, os.path.dirname(dest)))
EOF
"$src" "$dest")
  
  # Create symlink
  echo "  Creating symlink: $dest -> $rel_path"
  ln -s "$rel_path" "$dest"
  
  # Verify taxonomyPackage.xml exists
  package_manifest="$src/META-INF/taxonomyPackage.xml"
  if [ -f "$package_manifest" ]; then
    echo "  ✓ Found taxonomyPackage.xml"
  else
    echo "  ⚠ WARNING: taxonomyPackage.xml not found at $package_manifest"
  fi
  
  echo "  ✓ $name symlink created successfully"
done

echo ""
echo "Symlink creation completed!"
echo "Verifying symlinks..."

# Verify all symlinks
for i in "${!NAMES[@]}"; do
  name="${NAMES[$i]}"
  dest="$DEST_BASE/$name"
  if [ -L "$dest" ] && [ -d "$dest" ]; then
    echo "✓ $name: OK ($(readlink "$dest"))"
  else
    echo "✗ $name: FAILED"
    exit 1
  fi
done

echo ""
echo "All RF 4.0 taxonomy package symlinks created successfully!"
echo "Symlinks are located at: $DEST_BASE"
