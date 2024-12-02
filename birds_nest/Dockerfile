FROM python:3-alpine

# The installer requires curl (and certificates) to download the release archive
RUN apk add --no-cache curl

# Download the latest installer
ADD https://astral.sh/uv/install.sh /uv-installer.sh

# Run the installer then remove it
RUN sh /uv-installer.sh && rm /uv-installer.sh

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

COPY . .

RUN uv sync --frozen

EXPOSE 8000

CMD ["uv","run", "manage.py", "runserver", "0.0.0.0:8000"]
