#!/usr/bin/env python
# -*- coding: utf-8 -*
"""
CMS-Monit notifier
Author: Christian Ariza, https://github.com/cronosnull/CMS-Monit-notifier
"""
try:
    import importlib.resources as pkg_resources
except ImportError:
    # Try backported to PY<37 `importlib_resources`.
    import importlib_resources as pkg_resources
import os
from datetime import datetime
import json
import logging
import requests
import click
from . import templates  # the package containing the json templates

logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


class Notifier:
    """
        This class is resposible to send notifications to
        the monit infrastructure.  It will use an post rest enpoint
        and sent the date in the format indicated in
        http://monit-docs.web.cern.ch/monit-docs/alarms/external.html
    """

    def __init__(self, config=None):
        """
        :param config (optional) a dictionary with the format defined
        in the readme, defining the diferent cases and the default case.
        """
        self._config = json.load(pkg_resources.open_text(templates, "default.json"))
        self._config.update(config or {})

    def send_notification(self, case=0, subject=None, description=None):
        """
            send a notification to the monit infrastructure given the case
            specified.
        """
        _data = {**self.__get_config(case)}
        _data.update({"summary": subject, "description": description})
        _data["timestamp"] = int(datetime.utcnow().timestamp())
        if "targets" in _data and len(_data["targets"]) > 0:
            self.__send_message(_data)
        else:
            logging.info("no message for exit_%d", case)

    def __send_message(self, data):
        endpoint = self._config.get("notification_endpoint", None)
        if not endpoint:
            raise ValueError(
                "there is no notification endpoint  neither in the config file nor the default"
            )
        logging.debug(json.dumps(data, indent=4))
        response = requests.post(
            endpoint,
            data=json.dumps(data),
            headers={"Content-type": "application/json", "charset": "utf-8"},
        )
        logging.debug(response.text)

    def __get_config(self, case=0):
        conf = None
        _key = "exit_%d" % case
        if "cases" in self._config:
            if _key in self._config.get("cases"):
                conf = self._config["cases"][_key]
        if conf is None:
            conf = self._config.get("default_case", {})
        return conf

    def get_current_config(self):
        """
            return a string with the current configuration values
        """
        return json.dumps(self._config, indent=4)


@click.command()
@click.option("--config_file", default=None, type=click.File("r", encoding="utf-8"),
              help="notification configuration file")
@click.option("--see_config", default=False, is_flag=True,
              help="Shows the configuration (combining the defaults and the specified config file) and exits")
@click.option("--subject", default="CMS Notifier message",
              help="Subject/summary of the notification")
@click.option("--case", default=0,
              help="case to be applied to this notification -defined as exit_{case} in the configuration")
def main(subject, config_file=None, see_config=False, case=0):
    """
    The notifier application will send a message to the monit
    infrastructure. It will recieve the message in the standard input,
    so it can be piped from other commands.
    """
    notifier = Notifier(json.load(config_file) if config_file else None)
    if see_config:
        print(notifier.get_current_config())
    else:
        _desc = click.get_text_stream("stdin").read()
        logging.debug("description: %s", _desc)
        _desc = _desc.lstrip() or "No message provided"
        notifier.send_notification(subject=subject, description=_desc, case=case)


if __name__ == "__main__":
    # Click injected function:
    main()  # pylint: disable=no-value-for-parameter
