# Container for testing.
FROM gitlab-ee.rtp.nutanix.com:4567/xray/charon-baseimage:latest

# Install prerequisites.
WORKDIR /curie
ADD requirements.txt .
ADD requirements-dev.txt .
RUN pip install -r requirements-dev.txt
