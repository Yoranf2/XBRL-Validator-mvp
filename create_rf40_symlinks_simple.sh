#!/usr/bin/env bash
set -euo pipefail

# Create RF 4.0 taxonomy package symlinks (simplified version)

ROOT="$(pwd)"
SRC_BASE="$ROOT/github_work/eba-taxonomies/taxonomies/4.0"
DEST_BASE="$ROOT/backend/github_work/taxonomies/eba/rf40"

echo "Creating RF 4.0 taxonomy package symlinks..."
echo "Source base: $SRC_BASE"
echo "Destination base: $DEST_BASE"

# Ensure destination directory exists
mkdir -p "$DEST_BASE"

# Create symlinks one by one
echo "Creating rf-unpacked symlink..."
SRC_RF="$SRC_BASE/EBA_XBRL_4.0_Reporting_Frameworks_4.0.0.0/EBA_XBRL_4.0_Reporting_Frameworks_4.0.0.0"
DEST_RF="$DEST_BASE/rf-unpacked"
if [ -e "$DEST_RF" ]; then rm -f "$DEST_RF"; fi
ln -s "../../../../../github_work/eba-taxonomies/taxonomies/4.0/EBA_XBRL_4.0_Reporting_Frameworks_4.0.0.0/EBA_XBRL_4.0_Reporting_Frameworks_4.0.0.0" "$DEST_RF"
echo "✓ rf-unpacked created"

echo "Creating dict-unpacked symlink..."
SRC_DICT="$SRC_BASE/EBA_XBRL_4.0_Dictionary_4.0.0.0/EBA_XBRL_4.0_Dictionary_4.0.0.0"
DEST_DICT="$DEST_BASE/dict-unpacked"
if [ -e "$DEST_DICT" ]; then rm -f "$DEST_DICT"; fi
ln -s "../../../../../github_work/eba-taxonomies/taxonomies/4.0/EBA_XBRL_4.0_Dictionary_4.0.0.0/EBA_XBRL_4.0_Dictionary_4.0.0.0" "$DEST_DICT"
echo "✓ dict-unpacked created"

echo "Creating severity-unpacked symlink..."
SRC_SEV="$SRC_BASE/EBA_XBRL_4.0_Severity_4.0.0.0/EBA_XBRL_4.0_Severity_4.0.0.0"
DEST_SEV="$DEST_BASE/severity-unpacked"
if [ -e "$DEST_SEV" ]; then rm -f "$DEST_SEV"; fi
ln -s "../../../../../github_work/eba-taxonomies/taxonomies/4.0/EBA_XBRL_4.0_Severity_4.0.0.0/EBA_XBRL_4.0_Severity_4.0.0.0" "$DEST_SEV"
echo "✓ severity-unpacked created"

echo ""
echo "Verifying symlinks..."

# Verify symlinks
if [ -L "$DEST_RF" ] && [ -d "$DEST_RF" ]; then
  echo "✓ rf-unpacked: OK"
  if [ -f "$DEST_RF/META-INF/taxonomyPackage.xml" ]; then
    echo "  ✓ taxonomyPackage.xml found"
  else
    echo "  ⚠ taxonomyPackage.xml not found"
  fi
else
  echo "✗ rf-unpacked: FAILED"
  exit 1
fi

if [ -L "$DEST_DICT" ] && [ -d "$DEST_DICT" ]; then
  echo "✓ dict-unpacked: OK"
  if [ -f "$DEST_DICT/META-INF/taxonomyPackage.xml" ]; then
    echo "  ✓ taxonomyPackage.xml found"
  else
    echo "  ⚠ taxonomyPackage.xml not found"
  fi
else
  echo "✗ dict-unpacked: FAILED"
  exit 1
fi

if [ -L "$DEST_SEV" ] && [ -d "$DEST_SEV" ]; then
  echo "✓ severity-unpacked: OK"
  if [ -f "$DEST_SEV/META-INF/taxonomyPackage.xml" ]; then
    echo "  ✓ taxonomyPackage.xml found"
  else
    echo "  ⚠ taxonomyPackage.xml not found"
  fi
else
  echo "✗ severity-unpacked: FAILED"
  exit 1
fi

echo ""
echo "All RF 4.0 taxonomy package symlinks created successfully!"
echo "Symlinks are located at: $DEST_BASE"
