## Arrow installation

We use the Arrow C api made available using glib wrappers around a C++ core. Hence, we need to install both the C++ and the Glib projects. We build from source until a packaged version is available that has all the features we need. 

NOTE: I don't use ubuntu so please sub the dependencies installed below with their Debian equivalents. 


### Getting the Arrow source
This directory has a source archive for arrow v3.0.0, with the last git commit id: `d613aa68789288d3503dfbd8376a41f2d28b6c9d`. If you want to build from the latest source, download it from github. 
```bash
git clone https://github.com/apache/arrow.git --depth=1
```

Extract and copy this directory over to a temporary location since it produces a lot of build artifacts and you don't want to commit that to github. 


### Arrow C++

```bash
sudo yum install cmake3
sudo ln -s /usr/bin/cmake3 /usr/bin/cmake

cd arrow/cpp
mkdir release
cd release
cmake .. -DARROW_COMPUTE=ON -DARROW_FILESYSTEM=ON -DARROW_CSV=ON -DARROW_JSON=ON  -DARROW_WITH_ZSTD=ON -DARROW_WITH_LZ4=ON
make arrow
sudo make install
```

### Arrow Glib

```bash
sudo yum install -y gtk-doc gobject-introspection-devel
sudo yum install autoconf-archive
sudo yum install gcc-c++

export PKG_CONFIG_PATH=$PKG_CONFIG_PATH:/usr/local/lib64/pkgconfig:/usr/local/lib64/pkgconfig

cd arrow/c_glib
./autogen.sh 
cd arrow-glib
sudo make install
```

After finising the installation, you might need to run `ldconfig` for the system to recognize the Arrow shared libraries. Something like: 

```bash
echo /usr/local/lib >> /etc/ld.so.conf.d/local.conf
echo /usr/local/lib64 >> /etc/ld.so.conf.d/local.conf
ldconfig
```


### Arrow type/function declarations to use from Dlang
NOTE: You shouldn't need to do this unless you're building from a different version of the Arrow source code and not the included archive. 

We use `dpp` to do translate C header files for the Arrow library to work in D. The installation has clang deps so done using a docker container using this github [tip](https://github.com/gtkd-developers/gir-to-d/issues/27#issuecomment-633810882). Copy over the Dockerfile to an empty directory and also copy over the header files from your local arrow installation, so it can use them.

```bash
mkdir /tmp/dpparrow
cp Dockerfile /tmp/dpparrow/
cp fix_headers.py /tmp/dpparrow/
cp -r /usr/include/arrow-glib /tmp/dpparrow/arrow-glib
cd /tmp/dpparrow
```

Use docker.
```bash
docker build -t dpparrow .
docker run -d --name dppuser dpparrow
docker cp dppuser:/tmp/arrow.d arrow_temp.d

docker stop dppuser
docker rmi dppuser:latest --force
```

#### Fixing the header file 
`Dpp` leaves some struct definitions incompatible with Dlang, so we fix them. 

```bash
cd /tmp/dpparrow
python fix_headers.py arrow_temp.d arrow_bindings.d
```

Now, you can copy over the D header file `arrow_bindings.d` for usage. There are some more harmless warnings which can be edited out from the file with D bindings. 
