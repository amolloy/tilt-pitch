import argparse
import asyncio
import signal
import threading
import time
import queue
import logging
import struct
import uuid
from types import SimpleNamespace
from pyfiglet import Figlet
from bleak import BleakScanner

from .models import TiltStatus
from .providers import *
from .configuration import PitchConfig
from .rate_limiter import RateLimitedException

#############################################
# Statics
#############################################
uuid_to_colors = {
    "a495bb20-c5b1-4b44-b512-1370f02d74de": "green",
    "a495bb30-c5b1-4b44-b512-1370f02d74de": "black",
    "a495bb10-c5b1-4b44-b512-1370f02d74de": "red",
    "a495bb60-c5b1-4b44-b512-1370f02d74de": "blue",
    "a495bb50-c5b1-4b44-b512-1370f02d74de": "orange",
    "a495bb70-c5b1-4b44-b512-1370f02d74de": "yellow",
    "a495bb40-c5b1-4b44-b512-1370f02d74de": "purple",
    "a495bb80-c5b1-4b44-b512-1370f02d74de": "pink",
    "a495bb40-c5b1-4b44-b512-1370f02d74df": "simulated"
}

colors_to_uuid = dict((v, k) for k, v in uuid_to_colors.items())

# Load config
config = PitchConfig.load()

# Default Providers
normal_providers = [
    PrometheusCloudProvider(config),
    FileCloudProvider(config),
    InfluxDbCloudProvider(config),
    InfluxDb2CloudProvider(config),
    BrewfatherCustomStreamCloudProvider(config),
    BrewersFriendCustomStreamCloudProvider(config),
    GrainfatherCustomStreamCloudProvider(config),
    TaplistIOCloudProvider(config),
    AzureIoTHubCloudProvider(config)
]

# Queue for holding incoming scans
pitch_q = queue.Queue(maxsize=config.queue_size)

# Apple Manufacturer ID for iBeacons
APPLE_MANUFACTURER_ID = 0x004C

class IBeaconParser:
    @staticmethod
    def parse(manufacturer_data):
        # Byte 0: 0x02 (Type: iBeacon)
        # Byte 1: 0x15 (Length: 21 bytes)
        # Bytes 2-17: UUID
        # Bytes 18-19: Major
        # Bytes 20-21: Minor
        # Byte 22: TX Power
        
        if len(manufacturer_data) < 23:
            return None
            
        if manufacturer_data[0] != 0x02 or manufacturer_data[1] != 0x15:
            return None

        uuid_bytes, major, minor, tx_power = struct.unpack(">16sHHb", manufacturer_data[2:23])
        
        return SimpleNamespace(
            uuid=str(uuid.UUID(bytes=uuid_bytes)),
            major=major,
            minor=minor,
            tx_power=tx_power
        )

def pitch_main(providers, timeout_seconds: int, simulate_beacons: bool, console_log: bool = True):
    if providers is None:
        providers = normal_providers

    _start_message()
    
    # Add webhooks
    webhook_providers = _get_webhook_providers(config)
    if webhook_providers:
        providers.extend(webhook_providers)
        
    # Start cloud providers
    print("Starting...")
    enabled_providers = list()
    for provider in providers:
        if provider.enabled():
            enabled_providers.append(provider)
            provider__start_message = provider.start()
            if not provider__start_message:
                provider__start_message = ''
            print("...started: {} {}".format(provider, provider__start_message))

    # Determine mode
    if simulate_beacons:
        # Simulation is synchronous, can run in main thread or separate thread
        threading.Thread(name='background', target=_start_beacon_simulation, daemon=True).start()
        _run_queue_consumer(enabled_providers, console_log, timeout_seconds)
    else:
        consumer_thread = threading.Thread(
            target=_run_queue_consumer, 
            args=(enabled_providers, console_log, timeout_seconds),
            daemon=True
        )
        consumer_thread.start()
        
        try:
            asyncio.run(_run_bleak_scanner(timeout_seconds))
        except KeyboardInterrupt:
            print("\n...stopped: Tilt Scanner (KeyboardInterrupt)")

async def _run_bleak_scanner(timeout_seconds):
    print("...started: Tilt scanner")
    
    def bleak_callback(device, advertising_data):
        if APPLE_MANUFACTURER_ID in advertising_data.manufacturer_data:
            raw_data = advertising_data.manufacturer_data[APPLE_MANUFACTURER_ID]
            
            packet = IBeaconParser.parse(raw_data)
            
            if packet:
                _beacon_callback(device.address, advertising_data.rssi, packet, advertising_data.manufacturer_data)

    scanner = BleakScanner(bleak_callback)
    
    await scanner.start()
    print("Ready! Listening for beacons")
    
    # Keep the loop alive until timeout or user kill
    # If timeout_seconds is 0, we loop forever
    start_time = time.time()
    
    try:
        while True:
            await asyncio.sleep(1.0)
            if timeout_seconds > 0 and (time.time() - start_time > timeout_seconds):
                break
    except asyncio.CancelledError:
        pass
    finally:
        await scanner.stop()
        print("...stopped: Scanner")

def _run_queue_consumer(enabled_providers, console_log, timeout_seconds):
    start_time = time.time()
    while True:
        try:
            _handle_pitch_queue(enabled_providers, console_log)
            
            # Check timeout (if applicable) to kill the thread
            if timeout_seconds > 0 and (time.time() - start_time > timeout_seconds):
                return
                
        except Exception as e:
            print(f"Error in consumer loop: {e}")
            time.sleep(1)

def _start_beacon_simulation():
    print("...started: Tilt Beacon Simulator")
    fake_packet = argparse.Namespace(**{
        'uuid': colors_to_uuid['simulated'],
        'major': 70,
        'minor': 1035
    })
    while True:
        _beacon_callback(None, None, fake_packet, dict())
        time.sleep(0.25)


def _beacon_callback(bt_addr, rssi, packet, additional_info):
    if pitch_q.full():
        return

    uuid_val = packet.uuid
    color = uuid_to_colors.get(uuid_val)
    
    if color:
        tilt_status = TiltStatus(color, packet.major, _get_decimal_gravity(packet.minor), config)
        if not tilt_status.temp_valid:
            print("Ignoring broadcast due to invalid temperature: {}F".format(tilt_status.temp_fahrenheit))
        elif not tilt_status.gravity_valid:
            print("Ignoring broadcast due to invalid gravity: " + str(tilt_status.gravity))
        else:
            pitch_q.put_nowait(tilt_status)


def _handle_pitch_queue(enabled_providers: list, console_log: bool):
    if config.queue_empty_sleep_seconds > 0 and pitch_q.empty():
        time.sleep(config.queue_empty_sleep_seconds)
        return

    if pitch_q.full():
        length = pitch_q.qsize()
        print("Queue is full ({} events), scans will be ignored".format(length))

    try:
        tilt_status = pitch_q.get(timeout=1) 
    except queue.Empty:
        return

    for provider in enabled_providers:
        try:
            start = time.time()
            provider.update(tilt_status)
            time_spent = time.time() - start
            if console_log:
                print("Updated provider {} for color {} took {:.3f} seconds".format(provider, tilt_status.color, time_spent))
        except RateLimitedException:
            print("Skipping update due to rate limiting for provider {} for color {}".format(provider, tilt_status.color))
        except Exception as e:
            print(e)
            
    if console_log:
        print(tilt_status.json())


def _get_decimal_gravity(gravity):
    return gravity * .001


def _get_webhook_providers(config: PitchConfig):
    webhook_providers = list()
    for url in config.webhook_urls:
        webhook_providers.append(WebhookCloudProvider(url, config))
    return webhook_providers


def _start_message():
    f = Figlet(font='slant')
    print(f.renderText('Pitch'))

def _trigger_graceful_termination(signalNumber, frame):
    raise Exception("Termination signal received")
