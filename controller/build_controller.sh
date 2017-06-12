docker rmi -f `docker images | grep test_controller | sed -e 's/ \+/ /g' | cut -d' ' -f3`
docker build -t test_controller .

