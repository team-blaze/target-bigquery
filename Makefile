pip: prep upload cleanup

prep:
	if [ -d "dist" ]; then mv dist old_dist; fi

dist:
	python3 setup.py sdist bdist_wheel

upload: dist
	python3 -m twine upload dist/*

cleanup:
	if [ -d "dist" ]; then rm -rf old_dist; fi

docker-build:
	docker build --build-arg GOOGLE_PROJECT_ID=${GOOGLE_PROJECT_ID} --build-arg SERVICE_ACCOUNT_JSON_BASE64=$(shell cat $$GOOGLE_APPLICATION_CREDENTIALS | base64) -t target_bigquery .

docker-test: docker-build
	docker run -it --rm --name target_bigquery target_bigquery

docker-shell: docker-build
	docker run -it --rm --name target_bigquery --volume ${PWD}:/target_bigquery target_bigquery /bin/bash
