import numpy

from tiled.client import from_uri

client = from_uri("http://localhost:8000")

client.write_array(access_tags=["public"], array=numpy.ones((10, 10)), key="A")
client.write_array(access_tags=["beamline_x_user"], array=numpy.ones((10, 10)), key="B")
client.write_array(access_tags=["facility_admin"], array=numpy.ones((10, 10)), key="C")
client.write_array(access_tags=["facility_user"], array=numpy.ones((10, 10)), key="D")
