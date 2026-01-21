# Solution: Homework 1 - Docker & SQL


## Question 1. Understanding Docker images

Run docker with the `python:3.13` image. Use an entrypoint `bash` to interact with the container.

What's the version of `pip` in the image?

## Steps:
- confirm docker version:
```bash
docker --version
```
- create the docker image:
``` bash
docker run -it --rm --entrypoint=bash python:3.13
```
- update packages:
```bash
apt update
```
- confirm python version:
```bash
python --version
```
- check pip version:
```bash
pip --version
```
**Answer: pip 25.3 from /usr/local/lib/python3.13/site-packages/pip (python 3.13)**





