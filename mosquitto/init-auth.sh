#!/bin/sh
# Builds the Mosquitto password file from environment variables,
# then starts the broker. Passwords are stored hashed, never in git.
set -e

for v in MQTT_CONSUMER_PASSWORD MQTT_TEMPERATURE_PASSWORD MQTT_METHANE_PASSWORD MQTT_VIBRATION_PASSWORD; do
  eval "val=\$$v"
  if [ -z "$val" ]; then
    echo "Missing $v. Copy .env.example to .env and set it." >&2
    exit 1
  fi
done

PASSWD=/mosquitto/data/passwd
rm -f "$PASSWD"
mosquitto_passwd -c -b "$PASSWD" consumer "$MQTT_CONSUMER_PASSWORD"
mosquitto_passwd -b "$PASSWD" sensor-temperature "$MQTT_TEMPERATURE_PASSWORD"
mosquitto_passwd -b "$PASSWD" sensor-methane "$MQTT_METHANE_PASSWORD"
mosquitto_passwd -b "$PASSWD" sensor-vibration "$MQTT_VIBRATION_PASSWORD"

cp /mosquitto/config/acl /mosquitto/data/acl
chown mosquitto:mosquitto "$PASSWD" /mosquitto/data/acl
chmod 0700 "$PASSWD" /mosquitto/data/acl

exec /usr/sbin/mosquitto -c /mosquitto/config/mosquitto.conf
