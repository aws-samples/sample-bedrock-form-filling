#!/bin/bash

#############################################################################
# BDA Serverless Workflow - Unified Deployment Script
#
# This script:
# 1. Prompts for AWS configuration
# 2. Builds SAM application
# 3. Deploys CloudFormation stack (infrastructure + Cognito + CloudFront)
# 4. Creates test user with credentials
# 5. Uploads frontend to S3
# 6. Invalidates CloudFront cache
# 7. Displays CloudFront URL
#############################################################################

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}BDA Serverless Workflow Deployment${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Prompt for AWS configuration
echo -e "${YELLOW}AWS Configuration:${NC}"

read -p "Deployment region (default: us-east-1): " REGION
REGION="${REGION:-us-east-1}"

read -p "Stack name (default: bda-media-processing): " STACK_NAME
STACK_NAME="${STACK_NAME:-bda-media-processing}"

echo ""
echo -e "${GREEN}Configuration Summary:${NC}"
echo -e "  Region: ${GREEN}${REGION}${NC}"
echo -e "  Stack Name: ${GREEN}${STACK_NAME}${NC}"
echo -e "  ${BLUE}Note: BDA Project will be created automatically by CloudFormation${NC}"
echo ""
read -p "Continue with deployment? (y/N): " CONFIRM
if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Deployment cancelled.${NC}"
    exit 0
fi
echo ""

# Step 1: Build SAM application
echo -e "${YELLOW}[1/7] Building SAM application...${NC}"
cd infrastructure
sam build
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Build successful${NC}"
else
    echo -e "${RED}✗ Build failed${NC}"
    exit 1
fi
echo ""

# Step 2: Deploy CloudFormation stack
echo -e "${YELLOW}[2/7] Deploying CloudFormation stack...${NC}"
echo -e "${BLUE}This may take 5-10 minutes...${NC}"

sam deploy \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --parameter-overrides \
        "EnvironmentName=prod" \
    --capabilities CAPABILITY_IAM CAPABILITY_NAMED_IAM \
    --resolve-s3 \
    --no-confirm-changeset

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Stack deployed successfully${NC}"
else
    echo -e "${RED}✗ Deployment failed${NC}"
    exit 1
fi
echo ""

# Step 3: Get stack outputs
echo -e "${YELLOW}[3/7] Retrieving stack outputs...${NC}"

USER_POOL_ID=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].Outputs[?OutputKey==`UserPoolId`].OutputValue' \
    --output text)

CLIENT_ID=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].Outputs[?OutputKey==`UserPoolClientId`].OutputValue' \
    --output text)

API_ENDPOINT=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].Outputs[?OutputKey==`APIEndpoint`].OutputValue' \
    --output text)

FRONTEND_BUCKET=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].Outputs[?OutputKey==`FrontendBucketName`].OutputValue' \
    --output text)

DISTRIBUTION_ID=$(aws cloudformation describe-stacks \
    --stack-name "${STACK_NAME}" \
    --region "${REGION}" \
    --query 'Stacks[0].Outputs[?OutputKey==`CloudFrontDistributionId`].OutputValue' \
    --output text)

# Construct CloudFront URL from distribution domain name
CLOUDFRONT_DOMAIN=$(aws cloudfront get-distribution \
    --id "${DISTRIBUTION_ID}" \
    --query 'Distribution.DomainName' \
    --output text)
CLOUDFRONT_URL="https://${CLOUDFRONT_DOMAIN}"

if [ -z "$USER_POOL_ID" ] || [ -z "$CLIENT_ID" ] || [ -z "$FRONTEND_BUCKET" ]; then
    echo -e "${RED}✗ Failed to retrieve stack outputs${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Stack outputs retrieved${NC}"
echo ""

# Step 4: Create test user and set permanent password
echo -e "${YELLOW}[4/7] Creating test user and setting password...${NC}"

# Create the user
aws cognito-idp admin-create-user \
    --user-pool-id "${USER_POOL_ID}" \
    --username "test@example.com" \
    --user-attributes Name=email,Value=test@example.com Name=email_verified,Value=true \
    --message-action SUPPRESS \
    --region "${REGION}" 2>/dev/null && \
    echo -e "${GREEN}✓ Test user created${NC}" || \
    echo -e "${YELLOW}⚠ User may already exist, continuing...${NC}"

# Set permanent password
aws cognito-idp admin-set-user-password \
    --user-pool-id "${USER_POOL_ID}" \
    --username "test@example.com" \
    --password "Test123!" \
    --permanent \
    --region "${REGION}" 2>/dev/null && \
    echo -e "${GREEN}✓ Test user password set to: Test123!${NC}" || \
    echo -e "${YELLOW}⚠ Warning: Could not set password${NC}"
echo ""

# Step 5: Create frontend config.js
echo -e "${YELLOW}[5/7] Creating frontend configuration...${NC}"

cd ../frontend
cat > config.js <<EOF
// Auto-generated configuration
// Generated on: $(date)
// Stack: ${STACK_NAME}

window.API_ENDPOINT = '${API_ENDPOINT}';
window.COGNITO_USER_POOL_ID = '${USER_POOL_ID}';
window.COGNITO_CLIENT_ID = '${CLIENT_ID}';
window.COGNITO_REGION = '${REGION}';
EOF

echo -e "${GREEN}✓ Created frontend/config.js${NC}"
echo ""

# Step 6: Upload frontend to S3
echo -e "${YELLOW}[6/7] Uploading frontend to S3...${NC}"

aws s3 sync . "s3://${FRONTEND_BUCKET}/" \
    --exclude "*.example" \
    --exclude "AUTH_SETUP.md" \
    --exclude ".DS_Store" \
    --region "${REGION}"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Frontend uploaded to S3${NC}"
else
    echo -e "${RED}✗ Frontend upload failed${NC}"
    exit 1
fi
echo ""

# Step 7: Invalidate CloudFront cache
echo -e "${YELLOW}[7/7] Invalidating CloudFront cache...${NC}"

INVALIDATION_ID=$(aws cloudfront create-invalidation \
    --distribution-id "${DISTRIBUTION_ID}" \
    --paths "/*" \
    --query 'Invalidation.Id' \
    --output text)

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ CloudFront cache invalidated (ID: ${INVALIDATION_ID})${NC}"
else
    echo -e "${YELLOW}⚠ Warning: CloudFront invalidation failed (changes may take time to propagate)${NC}"
fi
echo ""

# Display success information
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}🎉 Deployment Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo -e "${BLUE}Your CloudFront URL:${NC}"
echo -e "  ${GREEN}${CLOUDFRONT_URL}${NC}"
echo ""
echo -e "${BLUE}Test Credentials:${NC}"
echo -e "  Username: ${GREEN}test@example.com${NC}"
echo -e "  Password: ${GREEN}Test123!${NC}"
echo ""
echo -e "${YELLOW}⏳ Note:${NC} CloudFront distribution may take 5-15 minutes to fully deploy."
echo -e "   If the URL doesn't work immediately, wait and try again."
echo ""
echo -e "${BLUE}Next Steps:${NC}"
echo -e "  1. Open ${GREEN}${CLOUDFRONT_URL}${NC}"
echo -e "  2. Login with the test credentials above"
echo -e "  3. Upload a media file (audio, video, document, or image) and watch the workflow in action"
echo ""
echo -e "${BLUE}Stack: ${GREEN}${STACK_NAME}${NC} | Region: ${GREEN}${REGION}${NC}"
echo ""
