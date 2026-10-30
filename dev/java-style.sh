#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
mode=${1:-check}
case "$mode" in
  check) ;;
  format) ;;
  *) echo 'Usage: dev/java-style.sh [check|format]' >&2; exit 2 ;;
esac

# This version runs on Java 8. Omit --aosp to use Google Java Style.
formatter_version=1.7
maven_repo=$(mvn -q help:evaluate -Dexpression=settings.localRepository -DforceStdout)
formatter_jar="$maven_repo/com/google/googlejavaformat/google-java-format/$formatter_version/google-java-format-$formatter_version-all-deps.jar"
if [ ! -f "$formatter_jar" ]; then
  mvn -B dependency:get \
    "-Dartifact=com.google.googlejavaformat:google-java-format:$formatter_version:jar:all-deps" \
    -Dtransitive=false
fi

# Google Java Format puts a single field annotation on the declaration line.
# Preserve the project's rule that @Resource occupies its own line.
formatted_root=$(mktemp -d)
trap 'rm -rf "$formatted_root"' EXIT HUP INT TERM
find src/main/java src/test/java -name '*.java' -type f > "$formatted_root/sources"
while IFS= read -r source_file; do
  mkdir -p "$formatted_root/$(dirname "$source_file")"
  cp "$source_file" "$formatted_root/$source_file"
done < "$formatted_root/sources"

find "$formatted_root/src" -name '*.java' -type f -print0 \
  | xargs -0 java -jar "$formatter_jar" --replace

layout_failed=0
while IFS= read -r source_file; do
  formatted_file="$formatted_root/$source_file"
  sed -E 's/^([[:blank:]]*)@Resource[[:blank:]]+((private|protected|public)[[:blank:]])/\1@Resource\
\1\2/' "$formatted_file" > "$formatted_file.normalized"
  mv "$formatted_file.normalized" "$formatted_file"
  if ! cmp -s "$source_file" "$formatted_file"; then
    if [ "$mode" = format ]; then
      cp "$formatted_file" "$source_file"
    else
      diff -u "$source_file" "$formatted_file" || true
      layout_failed=1
    fi
  fi
done < "$formatted_root/sources"

if [ "$layout_failed" -ne 0 ]; then
  exit 1
fi

# Formatting does not fix wildcard imports or multiple-variable declarations.
mvn -B validate
