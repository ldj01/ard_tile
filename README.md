# ard-tile

The Analysis-Ready Data (ARD) tiling project generates image tiles based on
Landsat scenes processed to Level 2 (surface reflectance and temperature).

## Installation

The system contains three components:

 - tile generation framework (Mesos framework)  
 - tile generator  
 - controller

The tiling framework and tile generator are installed into Docker
containers to be run in a Mesos cluster, whereas the controller
is installed onto a workstation or server with access to the Mesos cluster.
All three components interact with shared database tables to store and
retrieve information regarding the scenes being processed.

### Controller installation

The controller installation takes place on the designated workstation
or server, and is conducted as follows:

> `mkdir build` (if not already existing)   
  `cd build`  
  `cmake .. -CMAKE_INSTALL_PREFIX:PATH=/usr/local/usgs/ard_tile`  
  `make`  
  `sudo make install`

### Tile generation framework and application Docker images

Verify the version.txt files, and update if necessary:
> `external/version.txt`  
  `ARD_determine_segments_framework/version.txt`  
  `ARD_clip/version.txt`

Create and deploy the Docker images:

> `mkdir build` (if not already existing)  
  `cd build`  
  `cmake ..`  
  `make deploy`

If you want to test the containers before deployment, run the following
in lieu of "`make deploy`":

> `make ard-clip`  
  `make ard-segment`
