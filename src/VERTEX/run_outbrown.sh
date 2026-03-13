ENV_PATH="../../sinan.env"

if [ ! -f "$ENV_PATH" ]; then
    echo "Error: Arquivo .env não encontrado: $ENV_PATH" >&2
    exit 1
fi

while IFS= read -r line || [ -n "$line" ]; do
    trimmed_line=$(echo "$line" | xargs)
    
    if [ -z "$trimmed_line" ] || [[ "$trimmed_line" == \#* ]]; then
        continue
    fi
    
    IFS='=' read -r key value <<< "$trimmed_line"
    
    key=$(echo "$key" | xargs)
    value=$(echo "$value" | xargs)
    
    if [[ "$value" =~ ^\'.*\'$ ]] || [[ "$value" =~ ^\".*\"$ ]]; then
        value="${value:1:${#value}-2}"
    fi
    
    export "$key"="$value"
done < "$ENV_PATH"

python -m vertex.descriptive_dashboard