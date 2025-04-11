# Use AWS Lambda Python base image
FROM public.ecr.aws/lambda/python:3.13.2025.03.21.14-x86_64

# Set the working directory
WORKDIR /var/task

# Copy the requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV PREPROCESSED_BUCKET_NAME="recommended-preprocessed-data"
ENV MODEL_BUCKET_NAME="recommended-model-data"

# Copy the Lambda function code
COPY lambda_function.py .

# Command to run the Lambda function
CMD ["lambda_function.lambda_handler"]
