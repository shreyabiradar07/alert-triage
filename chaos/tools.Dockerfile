# Image for the `chaos` service: Docker's command line plus Python, so the scripts in chaos/ can run
# in a container and control the shop.
FROM docker:27-cli
RUN apk add --no-cache python3 bash curl
