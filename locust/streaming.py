import time
from locust import User, task, between, events
from tiled.client.stream import Subscription
from tiled.client import from_uri


class TiledStreamingUser(User):
    wait_time = between(0.5, 2.0)
    
    def on_start(self):
        # Hardcoded configuration
        self.client = from_uri("https://tiled-dev.nsls2.bnl.gov")
        self.target = "pil2M_image"
        # self.client = from_uri("http://localhost:8000", api_key="secret")
        # self.target = "img"
        
        # Track active subscriptions for cleanup
        self.subscriptions = []
        
        # Start the main catalog subscription
        self.catalog_sub = Subscription(self.client.context)
        self.catalog_sub.add_callback(self.on_new_run)
        self.catalog_sub.start()
        self.subscriptions.append(self.catalog_sub)
    
    def on_stop(self):
        # Clean up all subscriptions
        for sub in self.subscriptions:
            try:
                sub.stop()
            except Exception as e:
                print(f"Error stopping subscription: {e}")
    
    def on_new_run(self, sub, data):
        uid = data["key"]
        print(f"New run {uid}")
        
        # Record this as a successful event
        events.request.fire(
            request_type="websocket",
            name="new_run",
            response_time=0,
            response_length=0,
            exception=None,
            context={}
        )
        
        run_sub = Subscription(self.client.context, [uid], start=0)
        run_sub.add_callback(self.on_streams_namespace)
        run_sub.start()
        self.subscriptions.append(run_sub)
    
    def on_streams_namespace(self, sub, data):
        streams_sub = Subscription(self.client.context, sub.segments + ["streams"], start=0)
        streams_sub.add_callback(self.on_new_stream)
        streams_sub.start()
        self.subscriptions.append(streams_sub)
    
    def on_new_stream(self, sub, data):
        stream_name = data["key"]
        print(f"New stream {stream_name}")
        
        events.request.fire(
            request_type="websocket",
            name="new_stream",
            response_time=0,
            response_length=0,
            exception=None,
            context={}
        )
        
        stream_sub = Subscription(self.client.context, sub.segments + [stream_name], start=0)
        stream_sub.add_callback(self.on_node_in_stream)
        stream_sub.start()
        self.subscriptions.append(stream_sub)
    
    def on_node_in_stream(self, sub, data):
        key = data["key"]
        if key != self.target:
            return
            
        stream_sub = Subscription(self.client.context, sub.segments + [key], start=0)
        stream_sub.add_callback(self.load_data)
        stream_sub.start()
        self.subscriptions.append(stream_sub)
    
    def load_data(self, sub, data):
        try:
            patch = data['patch']
            slice_ = tuple(slice(offset, offset + shape) for offset, shape in zip(patch["offset"], patch["shape"]))
            node = self.client['/'.join(sub.segments)]
            slice_data = node.read(slice=slice_)
            print(slice_data)

            # Record successful data load
            events.request.fire(
                request_type="websocket",
                name="load_data",
                response_time=0,
                response_length=0,
                exception=None,
                context={}
            )
        except Exception as e:
            print(f"Error loading data: {e}")
            events.request.fire(
                request_type="websocket",
                name="load_data",
                response_time=0,
                response_length=0,
                exception=e,
                context={}
            )
    
    @task
    def keep_alive(self):
        # This task ensures Locust keeps the user active
        # The actual work is done through the subscription callbacks
        time.sleep(1)
