import os
import json
import time
import uuid
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

def create_producer():
    """Create and configure Kafka producer with retry logic"""
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[bootstrap_servers],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5,
                retry_backoff_ms=300,
                request_timeout_ms=30000,
                acks='all'
            )
            print(f"[PRODUCER] Successfully connected to Kafka at {bootstrap_servers}")
            return producer
        except Exception as e:
            print(f"[PRODUCER] Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def create_camera_topics():
    """Create dedicated topics for each camera"""
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    
    # Camera configurations
    cameras = [
        "CAM-QUAI-01",
        "CAM-QUAI-02", 
        "CAM-QUAI-03",
        "CAM-PORT-01",
        "CAM-PORT-02",
        "CAM-ENTRY-01"
    ]
    
    while True:
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=[bootstrap_servers],
                client_id='topic-creator'
            )
            
            # Create topic names based on camera IDs
            topics_to_create = []
            for camera in cameras:
                topic_name = f"{camera.lower().replace('-', '-')}-frames"
                topics_to_create.append(
                    NewTopic(
                        name=topic_name, 
                        num_partitions=1, 
                        replication_factor=1
                    )
                )
            
            # Create topics (ignore if they already exist)
            try:
                admin_client.create_topics(topics_to_create, validate_only=False)
                print(f"[PRODUCER] Created topics for {len(cameras)} cameras")
            except Exception as topic_error:
                print(f"[PRODUCER] Topics may already exist: {topic_error}")
            
            return cameras
            
        except Exception as e:
            print(f"[PRODUCER] Failed to create topics: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def generate_normalized_frame_event(camera_id):
    """Generate a NormalizedFrameReady event for specific camera"""
    # Add high-precision timestamp for latency calculation
    send_timestamp = time.time()
    
    return {
        "eventId": str(uuid.uuid4()),
        "timestamp": datetime.now().isoformat(),
        "missionId": "MISSION-SURVEILLANCE-2025",
        "assetId": camera_id,
        "frame_payload": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==",
        "send_timestamp": send_timestamp  # For latency calculation
    }

def get_topic_name(camera_id):
    """Convert camera ID to topic name"""
    return f"{camera_id.lower().replace('-', '-')}-frames"

def main():
    print("[PRODUCER] Starting Stream Normalizer...")
    
    # Create topics for all cameras
    cameras = create_camera_topics()
    
    # Create producer
    producer = create_producer()
    
    events_per_camera = 2
    total_events = len(cameras) * events_per_camera  # 6 cameras * 2 events = 12 total
    event_count = 0
    camera_event_counts = {camera: 0 for camera in cameras}
    
    # Track timing for overall latency
    first_message_time = None
    
    try:
        print(f"[PRODUCER] Will send {events_per_camera} events per camera ({total_events} total events)...")
        
        while event_count < total_events:
            # Find cameras that still need events
            available_cameras = [camera for camera in cameras if camera_event_counts[camera] < events_per_camera]
            
            if not available_cameras:
                break
                
            # Select random camera from available ones
            selected_camera = random.choice(available_cameras)
            topic_name = get_topic_name(selected_camera)
            
            # Generate event for selected camera
            event = generate_normalized_frame_event(selected_camera)
            
            # Track first message time
            if first_message_time is None:
                first_message_time = event["send_timestamp"]
            
            # Send to camera-specific topic
            producer.send(topic_name, event)
            event_count += 1
            camera_event_counts[selected_camera] += 1
            
            # Log the message (without send_timestamp to keep it clean)
            event_copy = {k: v for k, v in event.items() if k != 'send_timestamp'}
            print(f"[PRODUCER] Sent event {event_count}/{total_events} to topic '{topic_name}' (Camera {selected_camera}: {camera_event_counts[selected_camera]}/{events_per_camera}): {json.dumps(event_copy)}")
            
            # Wait before sending next event
            time.sleep(2)
        
        print(f"[PRODUCER] Successfully sent all {total_events} events ({events_per_camera} per camera). Shutting down...")
        
        # Write timing info to shared file for consumer to read
        timing_info = {
            "first_message_timestamp": first_message_time,
            "total_events_sent": total_events
        }
        
        with open('/tmp/producer_timing.json', 'w') as f:
            json.dump(timing_info, f)
        
    except KeyboardInterrupt:
        print("[PRODUCER] Shutting down...")
    except Exception as e:
        print(f"[PRODUCER] Error: {e}")
    finally:
        producer.close()
        print("[PRODUCER] Producer closed.")

if __name__ == "__main__":
    main()
