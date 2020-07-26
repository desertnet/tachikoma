#!/usr/bin/perl
use strict;
use warnings;
require './docker/config.pl';

docker_header();
docker_services();
docker_footer();

insecure();
