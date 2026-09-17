from tiled.client import from_uri

# tiled_staging = from_uri("https://tiled-staging.diamond.ac.uk")
tiled_staging = from_uri("http://127.0.0.1:8407")

# Create a Subscription
sub = tiled_staging.subscribe()

# Register a callback, a function that will be called when updates are received.
def on_child_created(update):
    print("NEW:", update.child())

sub.child_created.add_callback(on_child_created)

# Start listening for updates.
sub.start_in_thread()

# p46_1 = tiled_staging["p46/cm44194/1"]

# p46_1_sub = p46_1.subscribe()

# def on_new_data(update):
#     print(update)
#     print("NEW:", update.child())

# p46_1_sub.new_data.add_callback(on_new_data)