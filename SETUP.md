# Confluent SQL Setup Guide

This guide provides step-by-step instructions for setting up the `confluent-sql` library with Confluent Cloud Flink SQL services.

## Prerequisites

- A Confluent Cloud account
- Access to a Confluent Cloud environment with Flink enabled
- A **pre-created compute pool** in your Flink environment
- API credentials for Flink SQL API access

## Step 1: Find Your Environment and Organization IDs

### Environment ID
1. Log into [Confluent Cloud Console](https://confluent.cloud)
2. In the left sidebar, click on **"Environments"**
3. Select your environment
4. The Environment ID is displayed in the format: `env-xxxxxxxx`
5. **Copy this value** - you'll need it for the connection

### Organization ID
1. In the Confluent Cloud Console, look at the top-left corner
2. Click on your organization name
3. The Organization ID is displayed in the format: `org-xxxxxxxx`
4. **Copy this value** - you'll need it for the connection

## Step 2: Find Your Compute Pool ID

1. In the Confluent Cloud Console, navigate to **"Flink"** in the left sidebar
2. Select your **Environment** (if prompted)
3. Click on **"Compute Pools"**
4. Find your compute pool in the list
5. The Compute Pool ID is displayed in the format: `lfcp-xxxxxxxx`
6. **Copy this value** - you'll need it for the connection
7. **Ensure the compute pool is active** - the driver requires a running compute pool

## Step 3: Create Flink API Key (Required)

### Flink API Key (Required for SQL Operations)

1. In the Confluent Cloud Console, navigate to **"Flink"** in the left sidebar
2. Select your **Environment** and **Compute Pool**
3. Go to the **"API Keys"** tab in the Flink interface
4. Click **"Create API Key"**
5. Give it a name (e.g., "Flink SQL API Key")
6. Click **"Create"**
7. **IMPORTANT**: Copy both the API key and secret immediately - you won't be able to see the secret again!

### Optional: General Confluent Cloud API Key

If you plan to use compute pool management features in the future, you can also create a general Confluent Cloud API key:

1. In the Confluent Cloud Console, go to **"API Keys"** in the left sidebar
2. Click **"Create API Key"**
3. Give it a name (e.g., "General API Key")
4. Click **"Create"**
5. **Note**: This is optional for basic SQL operations

## Step 4: Find Your Region and Cloud Provider

1. In the Confluent Cloud Console, go to your **Environment** settings
2. Look for the **Region** information
3. Common values:
   - **Region**: `us-east-2`, `us-west-2`, `eu-west-1`, etc.
   - **Cloud Provider**: `aws`, `gcp`, `azure`

## Step 5: Test Your Connection

Create a test script to verify your credentials:

```python
import confluent_sql

# Replace with your actual values
connection = confluent_sql.connect(
    # Flink API credentials (required)
    flink_api_key="your-flink-api-key",
    flink_api_secret="your-flink-api-secret",
    
    # Connection parameters (all required)
    environment="env-xxxxxxxx",           # From Step 1
    organization_id="org-xxxxxxxx",       # From Step 1
    compute_pool_id="lfcp-xxxxxxxx",      # From Step 2
    region="us-east-2",                   # From Step 4
    cloud_provider="aws",                 # From Step 4
    
    # Optional: General Confluent Cloud API credentials
    api_key="your-general-api-key",       # Optional
    api_secret="your-general-api-secret"  # Optional
)

# Test the connection
cursor = connection.cursor()
cursor.execute("SELECT 1 as test_value")
result = cursor.fetchall()
print("Connection successful! Result:", result)

cursor.close()
connection.close()
```

## Step 6: Environment Variables Setup (Recommended)

For production use, set up environment variables:

```bash
# Required Flink API credentials
export FLINK_API_KEY="your-flink-api-key"
export FLINK_API_SECRET="your-flink-api-secret"

# Required connection parameters
export ENV_ID="env-xxxxxxxx"
export ORG_ID="org-xxxxxxxx"
export COMPUTE_POOL_ID="lfcp-xxxxxxxx"
export FLINK_REGION="us-east-2"

# Optional: General Confluent Cloud API credentials
export CONFLUENT_API_KEY="your-general-api-key"
export CONFLUENT_API_SECRET="your-general-api-secret"
```

Then use them in your code:

```python
import os
import confluent_sql

connection = confluent_sql.connect(
    # Required Flink API credentials
    flink_api_key=os.environ["FLINK_API_KEY"],
    flink_api_secret=os.environ["FLINK_API_SECRET"],
    
    # Required connection parameters
    environment=os.environ["ENV_ID"],
    compute_pool_id=os.environ["COMPUTE_POOL_ID"],
    organization_id=os.environ["ORG_ID"],
    region=os.environ["FLINK_REGION"],
    cloud_provider="aws",
    
    # Optional: General Confluent Cloud API credentials
    api_key=os.environ.get("CONFLUENT_API_KEY"),
    api_secret=os.environ.get("CONFLUENT_API_SECRET")
)
```

## Troubleshooting

### Common Issues

#### "401 Unauthorized" or "403 Forbidden"
- **Cause**: Invalid Flink API credentials
- **Solution**: Ensure you're using valid Flink API key and secret

#### "404 Compute pool not found"
- **Cause**: Incorrect compute pool ID or compute pool is not active
- **Solution**: Verify your `compute_pool_id` is correct and the compute pool is running

#### "404 Environment not found"
- **Cause**: Incorrect environment ID or no access to that environment
- **Solution**: Check your `environment` ID and ensure you have access to that environment

#### "404 Organization not found"
- **Cause**: Incorrect organization ID
- **Solution**: Verify your `organization_id` is correct

#### Connection timeouts or "nodename nor servname provided"
- **Cause**: Incorrect region or cloud provider
- **Solution**: Ensure `region` and `cloud_provider` match your Confluent Cloud setup

### Verification Checklist

Before running your code, verify you have:

**Required:**
- ✅ **Flink API Key** and **Secret** (for SQL operations)
- ✅ **Environment ID** (format: `env-xxxxxxxx`)
- ✅ **Organization ID** (format: `org-xxxxxxxx`)
- ✅ **Compute Pool ID** (format: `lfcp-xxxxxxxx`)
- ✅ **Region** (e.g., `us-east-2`)
- ✅ **Cloud Provider** (e.g., `aws`)

**Optional:**
- ⚪ **General Confluent Cloud API Key** and **Secret** (for future compute pool management)

### Getting Help

If you're still having issues:

1. **Check the Confluent Cloud Console** to ensure all resources are active
2. **Verify API key permissions** - ensure your Flink API key has the necessary permissions
3. **Check compute pool status** - ensure your compute pool is running
4. **Review the logs** - check for any error messages in the Confluent Cloud Console

## Security Best Practices

1. **Store credentials securely** using environment variables or a secrets manager
2. **Rotate API keys regularly** for better security
3. **Use the minimum required permissions** for your API keys
4. **Never commit credentials** to version control

## Next Steps

Once your connection is working:

1. **Explore the API**: Try different SQL statements
2. **Set up monitoring**: Monitor your compute pool usage
3. **Optimize performance**: Adjust compute pool settings as needed
4. **Scale up**: Add more compute pools for higher throughput

For more information, see the main [README.md](README.md) file.
