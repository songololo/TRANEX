import os
import shutil
import time
from pathlib import Path

import geopandas as gpd
import grass.script as gscript

mygisdb = "/tmp/grassdata"
mylocation = "world"
mymapset = "user"
import grass.script as grass
import psycopg2
from geopandas import read_file
from grass_session import Session
from shapely.geometry import Point


def run_tranex(
    temp_file_path: Path,
    db_config: dict[str, str],
    db_table_map: dict[str, str],
    grass_path: str,
    reflect: bool = True,
):
    """ """
    if reflect:
        if not temp_file_path.exists():
            temp_file_path.mkdir(exist_ok=True, parents=True)

    stt = time.time()
    # Connect to PostgreSQL
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    # Get number of points to process
    cur.execute(f"SELECT COUNT(*) FROM {db_table_map['receptors']}")
    n = cur.fetchone()[0]
    # Make output tables
    cur.execute("SELECT make_tables()")
    conn.commit()
    for i in range(1, n + 1):
        now = time.time()
        # Get this point
        cur.execute(
            f"""
            SELECT get_house({i}, '{db_table_map["receptors"]}', '{db_table_map["rec_fid"]}')
            """
        )
        conn.commit()
        # Calculate reflections
        if reflect:
            # Get viewpoint
            cur = con.cursor()
            cur.execute("SELECT ST_X(p.geom), ST_Y(p.geom) FROM this_point AS p")
            x, y = cur.fetchone()
            obs = (x, y)

            # Get heights
            q = f"SELECT get_rastersubset('{heights}', '{nodes}')"
            cur.execute(q)
            con.commit()

            # Check if any nodes present
            cur.execute("SELECT COUNT(*) FROM node_set")
            nc = cur.fetchone()[0]

            if nc > 0:
                try:
                    # Importing vector data into GRASS
                    gscript.run_command(
                        "v.in.ogr",
                        flags="o",
                        overwrite=True,
                        quiet=True,
                        input=dsn_st,
                        layer="build_hc",
                        type="boundary",
                        output="vDEM",
                    )
                    gscript.run_command("g.region", vector="vDEM")
                    gscript.run_command(
                        "v.to.rast",
                        flags="overwrite",
                        quiet=True,
                        type="area",
                        input="vDEM",
                        output="DEM",
                        use="attr",
                        attribute_column="val",
                    )

                    # Running viewshed analysis
                    gscript.run_command(
                        "r.viewshed",
                        flags="b",
                        overwrite=True,
                        quiet=True,
                        input="DEM",
                        output="vs_raster",
                        coordinates=obs,
                        observer_elevation=4,
                    )

                    # Converting raster to vector
                    gscript.run_command(
                        "r.to.vect",
                        input="vs_raster",
                        output="vvs",
                        type="area",
                        flags="overwrite",
                        quiet=True,
                    )
                    gscript.run_command(
                        "v.out.ogr",
                        flags="overwrite",
                        quiet=True,
                        input="vvs",
                        type="area",
                        output="shed.shp",
                    )

                    # Import and export nodes
                    gscript.run_command(
                        "v.in.ogr",
                        flags="o",
                        overwrite=True,
                        quiet=True,
                        input=dsn_st,
                        layer="node_set",
                        type="boundary",
                        output="vnode",
                    )
                    gscript.run_command(
                        "v.out.ogr",
                        flags="overwrite",
                        quiet=True,
                        input="vnode",
                        type="point",
                        output="vnodes.shp",
                    )

                    # Read the shapefiles using Geopandas
                    shed = gpd.read_file("shed.shp")
                    bldnode = gpd.read_file("vnodes.shp")

                    if bldnode is not None and shed is not None:
                        # Get building nodes in viewshed
                        bnode_in_shed = bldnode[bldnode.intersects(shed.unary_union)]
                        df = bnode_in_shed[["node_id"]]
                    else:
                        df = pd.DataFrame({"node_id": []})
                        print(
                            "No nodes in viewshed or receptor outside buildings raster"
                        )
                except Exception as e:
                    df = pd.DataFrame({"node_id": []})
                    print("Error in GRASS GIS operations:", e)
                # Example of reading shapefiles using Geopandas
                try:
                    shed = gpd.read_file("shed.shp")
                    bldnode = gpd.read_file("vnodes.shp")

                    if bldnode is not None and shed is not None:
                        # Get building nodes in viewshed
                        bnode_in_shed = bldnode[bldnode.intersects(shed.unary_union)]
                        df = bnode_in_shed[["node_id"]]
                    else:
                        df = pd.DataFrame({"node_id": []})
                        print(
                            "No nodes in viewshed or receptor outside buildings raster"
                        )
                except Exception as e:
                    df = pd.DataFrame({"node_id": []})
                    print("Error processing viewshed:", e)
            else:
                df = pd.DataFrame({"node_id": []})
                print("No viewshed for this point")

        # Run the noise model
        cur.execute(f"SELECT do_crtn('{rds}', '{lndc}', '{flw}')")
        laeq16 = cur.fetchone()[0]
        conn.commit()

        # Time taken and result for this point
        print(
            f"%%%%%% This point took: {round(time.time() - now, 4)}, Receptor: {i} of {n}, Laeq16: {round(laeq16, 4)} %%%%%%"
        )

    # Save to new shapefile
    cur.execute("DROP TABLE IF EXISTS output")
    cur.execute(
        f"CREATE TABLE output AS SELECT ST_X(r.geom), ST_Y(r.geom), d.* FROM noise AS d LEFT JOIN {rec} AS r ON d.rec_id = r.{rec_id}"
    )
    pth = os.path.join(d, "TRANEX_out.csv")
    cur.execute(f"COPY output TO '{pth}' DELIMITER ',' CSV HEADER")
    conn.commit()

    conn.close()

    shutil.rmtree(d, ignore_errors=True)

    print(f"OUTPUT IN: {pth}")
    print(f"TIME ELAPSED: {time.time() - stt}")
    print("##### FINISHED #####")


if __name__ == "__main__":
    # Example usage
    temp_file_path = Path(".") / "temp"
    db_config = {}
    db_table_map = {
        "receptors": "receptors",  # [Receptors]
        "rec_fid": "gid",  # [Receptor id field]
        "roads": "ne_10m_points_clip",  # [10m road segment points [rd_node_id]]
        "land": "mm_bh_ne_clip",  # [Landcover polygons]
        "flow": "traffic08_ne_clip",  # [Traffic dbf: v, q, p [rd_node_id]]
        "reflect": True,  # [Logical do reflections]
        "heights": "rat",  # [Building heights raster for reflections]
        "nodes": "ne_node50_clip",  # [Building polygon nodes for reflections]
    }

    run_tranex(
        temp_file_path,
        db_config,
        db_table_map,
    )
