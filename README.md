### StacEm
A STAC Directory created using WESM JSON. This document will walk you through the usage of this python project, `stacem`.

#### STAC Structure
In this project, we're creating a very straight forward STAC structure that's very similar to a normal directory structure you may use on your personal computer.

At the coarsest layer, we have the `Catalog`, which is written at `catalog.json` in your base directory. This `Catalog` organizes all of our projects underneath it, and houses the links to each one.

Each of these projects and the coarse metadata associated with it will be stored in a `Collection` within the project directory in a file called `collection.json`. This `Collection` will tell us about the project itself with key information like the date ranges for its creation and the bounding box for the data within it.

Each `Collection` will have a list of links to the individual STAC `Items`, which are the lowest level we'll get in this structure. Here, an `Item` references a specific `laz` file and accompanying `xml` document. Using the metadata from the WESM file, as well as information gathered from a `PDAL info` that's run over the pointcloud, we create a much more granular metadata file, with much more accurate bounding box information and information about the pointcloud itself.

The directory structure will look like this:

```
./
    - catalog.json
    - AK_ANCHORAGE_2015
        - collection.json
        - USGS_LPC_Anchorage_Lidar_63236780
            - USGS_LPC_Anchorage_Lidar_63236780.json
        - USGS_LPC_Anchorage_Lidar_63716807
            - USGS_LPC_Anchorage_Lidar_63716807.json
        ...
        - USGS_LPC_Anchorage_Lidar_63716806
            - USGS_LPC_Anchorage_Lidar_63716806.json
```

#### Usage
In order to create this structure, we'll use 2 different python modules: `stacem_create` and `stacem_finalize`.

##### stacem_create
The `Create` module will build our structure from the ground up, starting with leaf nodes. `Create` will iterate through each project and process all of the pointclouds that are found in the `lpc` link key from the WESM JSON object. It will pluck out the necessary information from the `PDAL info` call, including stats about the point schema, the coordinate systems, and bounding box. If something goes wrong with this part, we will write out any errors to the `errors` key in the STAC `Item` and continue to publishing it. This will allow us to track any problems with pointcloud files much easier.

Once each `Item` has been created for a specific project, a `Collection` will be made using those `Items` as children, and a `collection.json` file will be written to the local project directory.

##### stacem_finalize
The `Finalize` command will troll through the directory structure that we created in the `Create` step, and will collect all of the `Collections` into a list, to be added as children to our overarching `Catalog`. This `Catalog` will then be written out to the base directory as a `catalog.json` file.

##### Updating
This project was created with the goal of continuously updating one structure. On the schedule selected by the user, this module can be rerun over a previously made structure, updating any key-value pairs that have been added, as well as looking at `ETag` values of pointcloud files to determine whether or not a new run of `pdal info` is needed.

If there is nothing new being added, and no reprocessessing occurs, then the `Create` module will effectily be copying any data from s3 to your local machine.

##### Pushing to S3
Once you've created your local structure, all you need to do is perform something akin to a `s3 sync` call. `s3 sync` is a good function to have on deck, because it will not perform any unnecessary upload operations.