#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo "Usage: $0 SNAPSHOT_DATE CONTRACTHIST_ZIP PROCHIST_ZIP" >&2
    exit 2
fi

snapshot_date="$1"
contract_zip="$2"
procurement_zip="$3"
profile="${AWS_PROFILE:-new-account}"
region="${AWS_REGION:-us-east-1}"
bucket="a-and-d-intel-lake-newaccount"
bronze_prefix="bronze/dla/monthly_procurement/source_snapshot_date=${snapshot_date}"

if [[ ! "$snapshot_date" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "SNAPSHOT_DATE must use YYYY-MM-DD format." >&2
    exit 2
fi

for source_file in "$contract_zip" "$procurement_zip"; do
    if [[ ! -f "$source_file" ]]; then
        echo "Source archive not found: $source_file" >&2
        exit 2
    fi
done

contract_member="$(unzip -Z1 "$contract_zip")"
procurement_member="$(unzip -Z1 "$procurement_zip")"

if [[ "$(printf '%s\n' "$contract_member" | wc -l | tr -d ' ')" != "1" ]] ||
   [[ "$(printf '%s\n' "$procurement_member" | wc -l | tr -d ' ')" != "1" ]]; then
    echo "Each source archive must contain exactly one file." >&2
    exit 2
fi

if [[ "$contract_member" != *_contracthist.txt ]] ||
   [[ "$procurement_member" != *_prochist.txt ]]; then
    echo "Unexpected source archive member names." >&2
    exit 2
fi

echo "Uploading immutable source archives..."
aws s3 cp "$contract_zip" \
    "s3://${bucket}/${bronze_prefix}/archives/$(basename "$contract_zip")" \
    --profile "$profile" --region "$region" --only-show-errors
aws s3 cp "$procurement_zip" \
    "s3://${bucket}/${bronze_prefix}/archives/$(basename "$procurement_zip")" \
    --profile "$profile" --region "$region" --only-show-errors

echo "Extracting source text into the snapshot landing paths..."
unzip -p "$contract_zip" "$contract_member" | aws s3 cp - \
    "s3://${bucket}/${bronze_prefix}/contracthist/${contract_member}" \
    --profile "$profile" --region "$region" --only-show-errors
unzip -p "$procurement_zip" "$procurement_member" | aws s3 cp - \
    "s3://${bucket}/${bronze_prefix}/prochist/${procurement_member}" \
    --profile "$profile" --region "$region" --only-show-errors

echo "Uploaded DLA snapshot ${snapshot_date}."
echo "Contract history: s3://${bucket}/${bronze_prefix}/contracthist/${contract_member}"
echo "Procurement history: s3://${bucket}/${bronze_prefix}/prochist/${procurement_member}"
