import os
import json
import time
from kafka import KafkaConsumer

def create_consumer():
    """Create and configure Kafka consumer for all camera topics"""
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
    
    # Generate all camera topic names
    camera_topics = [f"{camera.lower().replace('-', '-')}-frames" for camera in cameras]
    
    while True:
        try:
            consumer = KafkaConsumer(
                *camera_topics,  # Subscribe to all camera topics
                bootstrap_servers=[bootstrap_servers],
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='phenomenon-detector-group',
                auto_offset_reset='latest',
                enable_auto_commit=True,
                consumer_timeout_ms=5000
            )
            print(f"[CONSUMER] Successfully connected to Kafka at {bootstrap_servers}")
            print(f"[CONSUMER] Subscribed to {len(camera_topics)} camera topics: {camera_topics}")
            return consumer
        except Exception as e:
            print(f"[CONSUMER] Failed to connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def simulate_analysis(event, event_count, max_events, topic_name, latencies):
    """Simulate AI analysis of the normalized frame"""
    receive_timestamp = time.time()
    
    event_id = event.get('eventId', 'unknown')
    asset_id = event.get('assetId', 'unknown')
    mission_id = event.get('missionId', 'unknown')
    send_timestamp = event.get('send_timestamp')
    
    # Calculate latency if send_timestamp is available
    latency_ms = None
    if send_timestamp:
        latency_ms = (receive_timestamp - send_timestamp) * 1000  # Convert to milliseconds
        latencies.append({
            'event_id': event_id,
            'asset_id': asset_id,
            'latency_ms': latency_ms,
            'send_timestamp': send_timestamp,
            'receive_timestamp': receive_timestamp
        })
    
    # Show message info (without send_timestamp to keep it clean)
    event_display = {k: v for k, v in event.items() if k != 'send_timestamp'}
    latency_info = f" (Latency: {latency_ms:.2f}ms)" if latency_ms else ""
    print(f"[CONSUMER] Received event {event_count}/{max_events} from topic '{topic_name}'{latency_info}: {json.dumps(event_display)}")
    print(f"[CONSUMER] Analyzing {asset_id} for mission {mission_id} on dedicated topic...")
    
    # Simulate processing time
    time.sleep(0.5)
    
    print(f"[CONSUMER] Analysis complete for event {event_count}/{max_events}: {event_id} from {asset_id}")
    
    return receive_timestamp

def log_latency_statistics(latencies, first_message_time, last_receive_time):
    """Log comprehensive latency statistics"""
    if not latencies:
        print("[CONSUMER] No latency data available.")
        return
    
    print("\n" + "="*80)
    print("LATENCY ANALYSIS REPORT")
    print("="*80)
    
    # Individual message latencies
    print(f"\nIndividual Message Latencies ({len(latencies)} messages):")
    print("-" * 60)
    for i, lat in enumerate(latencies, 1):
        print(f"{i:2d}. Event {lat['event_id'][:8]}... from {lat['asset_id']}: {lat['latency_ms']:6.2f}ms")
    
    # Statistical analysis
    latency_values = [lat['latency_ms'] for lat in latencies]
    avg_latency = sum(latency_values) / len(latency_values)
    min_latency = min(latency_values)
    max_latency = max(latency_values)
    
    print(f"\nLatency Statistics:")
    print("-" * 30)
    print(f"Average Latency: {avg_latency:.2f}ms")
    print(f"Minimum Latency: {min_latency:.2f}ms")
    print(f"Maximum Latency: {max_latency:.2f}ms")
    print(f"Total Messages:  {len(latencies)}")
    
    # Overall processing time
    if first_message_time and last_receive_time:
        total_processing_time = (last_receive_time - first_message_time) * 1000  # Convert to ms
        print(f"\nEnd-to-End Processing Time:")
        print("-" * 35)
        print(f"First Message Sent:     {time.strftime('%H:%M:%S.%f', time.localtime(first_message_time))[:-3]}")
        print(f"Last Message Received:  {time.strftime('%H:%M:%S.%f', time.localtime(last_receive_time))[:-3]}")
        print(f"Total Processing Time:  {total_processing_time:.2f}ms ({total_processing_time/1000:.2f}s)")
    
    print("="*80)

def main():
    print("[CONSUMER] Starting Phenomenon Detector...")
    consumer = create_consumer()
    
    event_count = 0
    max_events = 12  # 6 cameras * 2 events per camera
    max_wait_cycles = 30
    wait_cycles = 0
    
    # Latency tracking
    latencies = []
    last_receive_time = None
    
    try:
        print(f"[CONSUMER] Will process {max_events} events (2 per camera) then stop...")
        print("[CONSUMER] Waiting for messages from all camera topics...")
        
        while event_count < max_events:
            message_batch = consumer.poll(timeout_ms=1000)
            
            if message_batch:
                for topic_partition, messages in message_batch.items():
                    topic_name = topic_partition.topic
                    for message in messages:
                        event_count += 1
                        last_receive_time = simulate_analysis(message.value, event_count, max_events, topic_name, latencies)
                        
                        if event_count >= max_events:
                            break
                    if event_count >= max_events:
                        break
                wait_cycles = 0
            else:
                wait_cycles += 1
                if wait_cycles >= max_wait_cycles:
                    print(f"[CONSUMER] Timeout: No messages received after {max_wait_cycles} cycles")
                    break
        
        # Load producer timing info
        first_message_time = None
        try:
            with open('/tmp/producer_timing.json', 'r') as f:
                timing_info = json.load(f)
                first_message_time = timing_info.get('first_message_timestamp')
        except FileNotFoundError:
            print("[CONSUMER] Warning: Could not load producer timing information")
        
        if event_count >= max_events:
            print(f"[CONSUMER] Successfully processed all {max_events} events. Shutting down...")
        else:
            print(f"[CONSUMER] Processed {event_count}/{max_events} events before timeout.")
        
        # Log latency statistics before shutdown
        log_latency_statistics(latencies, first_message_time, last_receive_time)
                
    except KeyboardInterrupt:
        print("[CONSUMER] Shutting down...")
        # Still log statistics on interrupt
        first_message_time = None
        try:
            with open('/tmp/producer_timing.json', 'r') as f:
                timing_info = json.load(f)
                first_message_time = timing_info.get('first_message_timestamp')
        except FileNotFoundError:
            pass
        log_latency_statistics(latencies, first_message_time, last_receive_time)
    except Exception as e:
        print(f"[CONSUMER] Error: {e}")
    finally:
        consumer.close()
        print("[CONSUMER] Consumer closed.")

if __name__ == "__main__":
    main()
