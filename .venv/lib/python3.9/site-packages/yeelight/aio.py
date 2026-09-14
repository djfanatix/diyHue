# encoding: utf8
import asyncio
import contextlib
import json
import logging
import socket
import sys
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Tuple

from .enums import LightType
from .main import _command_to_send_command
from .main import Bulb
from .main import BulbException
from .main import DEFAULT_PROPS

if sys.version_info[:2] < (3, 11):
    from async_timeout import timeout as asyncio_timeout
else:
    from asyncio import timeout as asyncio_timeout


_LOGGER = logging.getLogger(__name__)

TIMEOUT = 15
PING_INTERVAL = 60

KEY_CONNECTED = "connected"

RECONNECT_ERRORS = ("client quota exceeded", "invalid command")
BACKOFF_ERRORS = ("client quota exceeded",)


def _async_command(f):
    """
    A decorator that wraps a function and enables effects.

    This decorator can only be used with async functions
    because it needs to call an awaitable.
    """

    async def wrapper(*args, **kw):
        """A decorator that wraps a function and enables effects."""
        self = args[0]
        cmd = await self.async_send_command(
            *_command_to_send_command(
                self,
                *(await f(*args, **kw)),
                kw.get("effect", self.effect),
                kw.get("duration", self.duration),
                kw.get("power_mode", self.power_mode),
            )
        )
        result = cmd.get("result", [])
        if result:
            return result[0]

    return wrapper


class AsyncBulb(Bulb):
    """Asyncio support for Bulb."""

    def __init__(self, *args, **kwargs) -> None:
        # Asyncio
        super().__init__(*args, **kwargs)
        self._loop = asyncio.get_running_loop()
        self._async_callback: Optional[Callable[[dict], None]] = None
        self._async_pending_commands: Dict[int, asyncio.Future] = {}
        self._async_listen_task: Optional[asyncio.Task] = None
        self._async_reconnect_task: Optional[asyncio.Task] = None
        self._async_writer: Optional[asyncio.StreamWriter] = None
        self._async_reader: Optional[asyncio.StreamReader] = None
        self._async_cmd_id = 0
        self._async_command_lock = asyncio.Lock()
        self._socket_backoff = False
        self._listen_event: Optional[asyncio.Event] = None
        self._music_mode_params = None
        # Prevent sending disconnects if disconnecting for music mode connection
        self._music_expect_disconnect = False
        self._music_mode_lock = asyncio.Lock()

    async def async_send_command(self, method, params):
        """Send a command to the bulb and wait for the result."""
        # Prevent changes to music mode while sending a command
        # unless we are currently activating music mode
        # this prevents sending a command while we are trying to connect
        # and causing a hang while waiting for the response
        writer_before_lock = self._async_writer
        was_locked = self._async_command_lock.locked()
        if was_locked and method == "set_music":
            command_lock = contextlib.AsyncExitStack()
        else:
            command_lock = self._async_command_lock
        async with command_lock:
            if was_locked and self._async_writer is not writer_before_lock:
                # If the writer changed, it means we dropped the connection
                # while waiting for the lock
                raise BulbException(
                    "The connection was closed while waiting for the lock"
                )
            if self._music_mode_state:
                self._async_send_command(method, params, create_future=False)
                # We can't check if it worked, so we just assume it did
                if self._async_callback:
                    self._async_callback({"result": ["ok"]})
                return {"result": ["ok"]}
            _, future = self._async_send_command(method, params)
            async with asyncio_timeout(TIMEOUT):
                response = await future

        if "error" in response:
            raise BulbException(response["error"])

        return response

    def _async_send_command(
        self, method: str, params: Any, create_future: bool = True
    ) -> Tuple[int, Optional[asyncio.Future]]:
        """
        Send the command asynchronously.

        This function is not meant to be called directly. Use async_send_command instead.

        This function must be called within the event loop.
        """
        if not self._async_writer:
            raise BulbException("The write socket is closed")

        self._async_cmd_id += 1
        request_id = self._async_cmd_id
        future: Optional[asyncio.Future] = None

        if create_future:
            future = self._loop.create_future()
            self._async_pending_commands[request_id] = future

            def clean_up(future: asyncio.Future) -> None:
                if future.done():
                    self._async_pending_commands.pop(request_id, None)

            future.add_done_callback(clean_up)

        command = {"id": request_id, "method": method, "params": params}
        request = (json.dumps(command, separators=(",", ":")) + "\r\n").encode("utf8")
        _LOGGER.debug("%s: > %s", self, request)
        self._async_writer.write(request)
        _LOGGER.debug("%s: Finished _async_send_command", self)
        return request_id, future

    async def _async_run_listen(self):
        """Backend for async_listen."""
        _LOGGER.debug("%s: Starting listen task", self)
        while self._is_listening:
            try:
                _LOGGER.debug("%s: Starting connection loop", self)
                await self._async_connection_loop()
            finally:
                _LOGGER.debug("%s: Listen task finalizing", self)
                await self._async_close_reader_writer()
                # if its not related to either
                # 1. Music mode turning on/off
                # 2. light being turned off
                if (
                    self._async_callback
                    and not self._music_expect_disconnect
                    and not self._music_mode_lock.locked()
                ):
                    _LOGGER.debug("%s: Sending disconnect callback", self)
                    self._async_callback({KEY_CONNECTED: False})
                if self._is_listening:
                    # Don't backoff if we expect to be disconnected
                    # e.g. if the light is turned off
                    if not self._music_expect_disconnect:
                        await self._async_backoff()
                    await self._async_reconnect_loop()
                self._music_expect_disconnect = False

    async def _async_reconnect_loop(self):
        _LOGGER.debug("%s: Starting reconnect", self)
        while self._is_listening:
            try:
                async with asyncio_timeout(TIMEOUT):
                    reader, writer = await asyncio.open_connection(self._ip, self._port)
                await asyncio.sleep(0.1)
            except (asyncio.TimeoutError, socket.error) as ex:
                _LOGGER.debug(
                    "%s: Reconnected failed with %s, backing off",
                    self,
                    str(ex) or type(ex),
                )
                await asyncio.sleep(TIMEOUT)
            else:
                _LOGGER.debug("%s: Reconnected successfully", self)
                self._async_connected(writer, reader)
                if self._async_callback:
                    self._async_callback({KEY_CONNECTED: True})
                # If connection drops without the light being turned off
                # no longer in music mode if we were in it
                self._music_mode_state = False
                if self._music_mode:
                    # Need to run this separately as starting music mode cancels this task
                    # and starting music mode will require a connection to the bulb
                    self._async_music_task = asyncio.ensure_future(
                        self.async_start_music(reconnect=True)
                    )
                return

        _LOGGER.debug("%s: Reconnect loop stopped", self)

    async def _async_backoff(self):
        """Back off only if we had a previous failure without a success."""
        if self._socket_backoff:
            _LOGGER.debug("%s: Backing off %s seconds", self, TIMEOUT)
            await asyncio.sleep(TIMEOUT)
        self._socket_backoff = True

    async def _async_connection_loop(self) -> None:
        timeouts = 0
        ping_id = -1
        assert self._async_reader is not None
        while self._is_listening:
            try:
                _LOGGER.debug(
                    "%s: Waiting for line, music_mode_state: %s",
                    self,
                    self._music_mode_state,
                )
                if self._listen_event:
                    self._listen_event.set()
                if self._music_mode_state:
                    # Force clear backoff if we are in music mode
                    # We will not receive any messages
                    # so there is no opportunity to back off
                    self._socket_backoff = False
                async with asyncio_timeout(PING_INTERVAL + TIMEOUT):
                    line = await self._async_reader.readline()
            except asyncio.TimeoutError:
                # Since we can't get a response from the light in music mode
                # ping the light to keep the connection alive
                if self._music_mode_state:
                    _LOGGER.debug(
                        "%s: Pinging bulb in music mode after %s",
                        self,
                        PING_INTERVAL + TIMEOUT,
                    )
                    if self._last_properties["power"] == "on":
                        await self.async_turn_on()
                    else:
                        # This should rarely happen as music mode is by default turned off
                        # when the light is turned off
                        await self.async_turn_off(is_ping=True)
                    continue
                timeouts += 1
                if timeouts == 2:
                    _LOGGER.debug("%s: Timeout waiting for line", self)
                    return
                _LOGGER.debug(
                    "%s: No data in %s seconds, pinging bulb to make sure its still connected",
                    self,
                    PING_INTERVAL + TIMEOUT,
                )
                ping_id, _ = self._async_send_command(
                    "get_prop", ["power"], create_future=False
                )
                continue
            except socket.error as ex:
                _LOGGER.debug("%s: Socket error: %s", self, ex)
                return
            except ValueError as ex:
                _LOGGER.debug("%s: Overran buffer: %s", self, ex)
                return
            except BulbException as ex:
                _LOGGER.warning(
                    "%s: Socket unexpectedly closed out from under us: %s", self, ex
                )
                return

            if line and b"\n" not in line:
                _LOGGER.debug("%s: Partial read from bulb: %s", self, line)
                return
            elif line:
                self._socket_backoff = False
                _LOGGER.debug("%s: Success got line: %s", self, line)
                timeouts = 0
            else:
                _LOGGER.debug(
                    "%s: Bulb closed the connection, music_expect_disconnect: %s",
                    self,
                    self._music_expect_disconnect,
                )
                return

            try:
                decoded_line: Dict[str, Any] = json.loads(line.decode("utf8").rstrip())
            except ValueError:
                _LOGGER.error("%s: Invalid data: %s", self, line)
                continue

            if "id" in decoded_line:
                future = self._async_pending_commands.pop(decoded_line["id"], None)
                if future and not future.done():
                    future.set_result(decoded_line)
                elif decoded_line["id"] == ping_id:
                    _LOGGER.debug("%s: Ping result received: %s", self, decoded_line)
                    data = {"power": decoded_line["result"][0]}
                    self._set_last_properties(data, update=True)
                    data[KEY_CONNECTED] = True
                    assert self._async_callback is not None
                    self._async_callback(data)
                    continue

            if (
                "error" in decoded_line
                and decoded_line["error"].get("message") in RECONNECT_ERRORS
            ):
                message = decoded_line["error"]["message"]
                _LOGGER.debug(
                    "%s: %s, dropping connection and reconnecting", self, message
                )
                if message in BACKOFF_ERRORS:
                    # Force backoff since reconnect will not clear the quota right away
                    self._socket_backoff = True
                return

            if decoded_line.get("method") != "props":
                _LOGGER.debug("%s: props not in line: %s", self, line)
                continue

            # Update notification received
            _LOGGER.debug("%s: New props received: %s", self, decoded_line)
            self._set_last_properties(decoded_line["params"], update=True)
            decoded_data: Dict[str, Any] = decoded_line["params"]
            decoded_data.update({KEY_CONNECTED: True})
            try:
                assert self._async_callback is not None
                self._async_callback(decoded_data)
            except Exception:  # pylint: disable=broad-except
                _LOGGER.exception("Error while processing external callback")

    def _async_connected(
        self, writer: asyncio.StreamWriter, reader: asyncio.StreamReader
    ) -> None:
        """Called when we are successfully connected to the bulb."""
        self._async_cmd_id = 0
        self._async_writer = writer
        self._async_reader = reader

    async def async_listen(self, callback):
        """
        Listen to state update notifications.

        This function is blocking until a socket error occurred or being stopped by
        ``stop_listening``. It should be run in an ``asyncio`` task.

        The callback function should take one parameter, containing the new/updates
        properties. It will be called when ``last_properties`` is updated.

        Reconnection happens automaticlly if the socket is closed until
        async_stop_listening is called

        :param callable callback: A callback function to receive state update notification.
        """
        self._async_callback = callback
        try:
            async with asyncio_timeout(TIMEOUT):
                reader, writer = await asyncio.open_connection(self._ip, self._port)
            await asyncio.sleep(0.1)
        except asyncio.TimeoutError as ex:
            raise BulbException(
                f"Timed out trying to the the bulb at {self._ip}:{self._port}."
            ) from ex
        except socket.error as ex:
            raise BulbException(
                f"Failed to read from the socket at {self._ip}:{self._port}: {ex}."
            ) from ex

        self._is_listening = True
        self._async_connected(writer, reader)
        self._async_listen_task = asyncio.ensure_future(self._async_run_listen())
        self._async_callback({KEY_CONNECTED: True})

    def _async_stop_listen_task(self):
        if self._async_listen_task:
            self._async_listen_task.cancel()
            self._async_listen_task = None

    async def _async_close_reader_writer(self):
        for pending in list(self._async_pending_commands.values()):
            if not pending.done():
                pending.set_exception(BulbException("Connection closed"))
        self._async_pending_commands.clear()
        if self._async_writer:
            # This is called both in async_stop_listening and
            # when the connection is dropped. Clear out the writer
            # first so it doesn't try to close a already closed writer
            writer = self._async_writer
            self._async_writer = None

            # Need to ignore socket errors if it was dropped
            with contextlib.suppress(socket.error):
                writer.close()
                async with asyncio_timeout(TIMEOUT):
                    await writer.wait_closed()
        self._async_reader = None

    async def async_stop_listening(self, remove_callback=True):
        """Stop listening to notifications."""
        self._is_listening = False
        self._async_stop_listen_task()
        await self._async_close_reader_writer()
        if remove_callback:
            self._async_callback = None

    async def async_get_properties(
        self, requested_properties=DEFAULT_PROPS, when_on=False
    ):
        """
        Retrieve and return the properties of the bulb.

        This method also updates ``last_properties`` when it is called.

        The ``current_brightness`` property is calculated by the library (i.e. not returned
        by the bulb), and indicates the current brightness of the lamp, aware of night light
        mode. It is 0 if the lamp is off, and None if it is unknown.

        :param list requested_properties: The list of properties to request from the bulb.
                                          By default, this does not include ``flow_params``.
        :param bool when_on: Only refresh if on.

        :returns: A dictionary of param: value items.
        :rtype: dict
        """
        # when_on_music is only called when reconnecting to music mode
        # ignores music mode flag and only refreshes if on
        if (
            when_on and self._last_properties["power"] != "on"
        ) or self._music_mode_state:
            return self._last_properties

        response = await self.async_send_command("get_prop", requested_properties)
        # do a second check here because music mode could have been enabled
        # while the command was queued this would cause an incorrect response
        # {'power': 'ok', 'current_brightness': None}
        if self._music_mode_state:
            return self._last_properties
        if response is not None and "result" in response:
            properties = response["result"]
            properties = [x if x else None for x in properties]
            new_values = dict(zip(requested_properties, properties))

            # this was a music mode response, so ignore this update
            if new_values["power"] == "ok":
                return self._last_properties

            self._set_last_properties(new_values, update=False)

        return self._last_properties

    async def async_ensure_on(self):
        """Turn the bulb on if it is off."""
        if self._music_mode_state is True or self.auto_on is False:
            return

        await self.async_get_properties()

        if self._last_properties["power"] != "on":
            await self.async_turn_on()

    @_async_command
    async def async_set_color_temp(self, degrees, light_type=LightType.Main, **kwargs):
        """
        Set the bulb's color temperature.

        :param int degrees: The degrees to set the color temperature to (min/max are
                            specified by the model's capabilities, or 1700-6500).
        :param yeelight.LightType light_type: Light type to control.
        """
        await self.async_ensure_on()
        return self._set_color_temp(degrees, light_type=light_type, **kwargs)

    @_async_command
    async def async_set_rgb(
        self, red, green, blue, light_type=LightType.Main, **kwargs
    ):
        """
        Set the bulb's RGB value.

        :param int red:   The red value to set (0-255).
        :param int green: The green value to set (0-255).
        :param int blue:  The blue value to set (0-255).
        :param yeelight.LightType light_type:
                          Light type to control.
        """
        await self.async_ensure_on()
        return self._set_rgb(red, green, blue, light_type=light_type, **kwargs)

    @_async_command
    async def async_set_adjust(self, action, prop, **kwargs):
        """
        Adjust a parameter.

        I don't know what this is good for. I don't know how to use it, or why.
        I'm just including it here for completeness, and because it was easy,
        but it won't get any particular love.

        :param str action: The direction of adjustment. Can be "increase",
                           "decrease" or "circle".
        :param str prop:   The property to adjust. Can be "bright" for
                           brightness, "ct" for color temperature and "color"
                           for color. The only action for "color" can be
                           "circle". Why? Who knows.
        """
        return self._set_adjust(action, prop, **kwargs)

    @_async_command
    async def async_set_hsv(
        self, hue, saturation, value=None, light_type=LightType.Main, **kwargs
    ):
        """
        Set the bulb's HSV value.

        :param int hue:        The hue to set (0-359).
        :param int saturation: The saturation to set (0-100).
        :param int value:      The value to set (0-100). If omitted, the bulb's
                               brightness will remain the same as before the
                               change.
        :param yeelight.LightType light_type: Light type to control.
        """
        await self.async_ensure_on()
        return self._set_hsv(hue, saturation, value, light_type, **kwargs)

    @_async_command
    async def async_set_brightness(
        self, brightness, light_type=LightType.Main, **kwargs
    ):
        """
        Set the bulb's brightness.

        :param int brightness: The brightness value to set (1-100).
        :param yeelight.LightType light_type: Light type to control.
        """
        await self.async_ensure_on()
        return self._set_brightness(brightness, light_type=light_type, **kwargs)

    @_async_command
    async def async_turn_on(self, light_type=LightType.Main, **kwargs):
        """
        Turn the bulb on.

        :param yeelight.LightType light_type: Light type to control.
        """
        return self._turn_on(light_type=light_type, **kwargs)

    @_async_command
    async def async_turn_off(
        self, light_type=LightType.Main, is_ping: bool = False, **kwargs
    ):
        """
        Turn the bulb off.

        :param yeelight.LightType light_type: Light type to control.
        :param bool is_ping: If true, don't disable music mode.
        """
        # Turning off implicitly disables music mode.
        # Prevent sending disconnects
        if self._music_mode and not is_ping:
            self._music_expect_disconnect = True
            self._music_mode = False
        return self._turn_off(light_type=light_type, **kwargs)

    @_async_command
    async def async_toggle(self, light_type=LightType.Main, **kwargs):
        """
        Toggle the bulb on or off.

        :param yeelight.LightType light_type: Light type to control.
        """
        return self._toggle(light_type=light_type, **kwargs)

    @_async_command
    async def async_dev_toggle(self, **kwargs):
        """Toggle the main light and the ambient on or off."""
        return self._dev_toggle(**kwargs)

    @_async_command
    async def async_set_default(self, light_type=LightType.Main, **kwargs):
        """
        Set the bulb's current state as the default, which is what the bulb will be set to on power on.

        If you get a "general error" setting this, yet the bulb reports as supporting `set_default` during
        discovery, disable "auto save settings" in the YeeLight app.

        :param yeelight.LightType light_type: Light type to control.
        """
        return self._set_default(light_type=light_type, **kwargs)

    @_async_command
    async def async_set_name(self, name, **kwargs):
        """
        Set the bulb's name.

        :param str name: The string you want to set as the bulb's name.
        """
        return self._set_name(name, **kwargs)

    @_async_command
    async def async_start_flow(self, flow, light_type=LightType.Main, **kwargs):
        """
        Start a flow.

        :param yeelight.Flow flow: The Flow instance to start.
        """
        await self.async_ensure_on()
        return self._start_start_flow(flow, light_type=light_type, **kwargs)

    @_async_command
    async def async_stop_flow(self, light_type=LightType.Main, **kwargs):
        """
        Stop a flow.

        :param yeelight.LightType light_type: Light type to control.
        """
        return self._stop_flow(light_type=light_type, **kwargs)

    @_async_command
    async def async_set_scene(
        self, scene_class, *args, light_type=LightType.Main, **kwargs
    ):
        """
        Set the light directly to the specified state.

        If the light is off, it will first be turned on.

        :param yeelight.SceneClass scene_class: The YeeLight scene class to use.

        * `COLOR` changes the light to the specified RGB color and brightness.

            Arguments:
            * **red** (*int*)         – The red value to set (0-255).
            * **green** (*int*)       – The green value to set (0-255).
            * **blue** (*int*)        – The blue value to set (0-255).
            * **brightness** (*int*)  – The brightness value to set (1-100).

        * `HSV` changes the light to the specified HSV color and brightness.

            Arguments:
            * **hue** (*int*)         – The hue to set (0-359).
            * **saturation** (*int*)  – The saturation to set (0-100).
            * **brightness** (*int*)  – The brightness value to set (1-100).

        * `CT` changes the light to the specified color temperature.

            Arguments:
            * **degrees** (*int*)     – The degrees to set the color temperature to (min/max are specified by the
            model's capabilities, or 1700-6500).
            * **brightness** (*int*)  – The brightness value to set (1-100).

        * `CF` starts a color flow.

            Arguments:
            * **flow** (`yeelight.Flow`)  – The Flow instance to start.

        * `AUTO_DELAY_OFF` turns the light on to the specified brightness and sets a timer to turn it back off after the
          given number of minutes.

            Arguments:
            * **brightness** (*int*)     – The brightness value to set (1-100).
            * **minutes** (*int*)        – The minutes to wait before automatically turning the light off.

        :param yeelight.LightType light_type: Light type to control.
        """
        return self._set_scene(scene_class, *args, light_type=light_type, **kwargs)

    @_async_command
    async def async_cron_add(self, event_type, value, **kwargs):
        """
        Add an event to cron.

        Example::

        >>> bulb.cron_add(CronType.off, 10)

        :param yeelight.CronType event_type: The type of event. Currently,
                                                   only ``CronType.off``.
        """
        return self._cron_add(event_type, value, **kwargs)

    @_async_command
    async def async_cron_get(self, event_type, **kwargs):
        """
        Retrieve an event from cron.

        :param yeelight.CronType event_type: The type of event. Currently,
                                                   only ``CronType.off``.
        """
        return self._cron_get(event_type, **kwargs)

    @_async_command
    async def cron_del(self, event_type, **kwargs):
        """
        Remove an event from cron.

        :param yeelight.CronType event_type: The type of event. Currently,
                                                   only ``CronType.off``.
        """
        return self._cron_del(event_type, **kwargs)

    def __repr__(self):
        return "AsyncBulb<{ip}:{port}, type={type}, model={model}>".format(
            ip=self._ip, port=self._port, type=self.bulb_type, model=self.model
        )

    async def async_set_power_mode(self, mode):
        """
        Set the light power mode.

        If the light is off it will be turned on.

        :param yeelight.PowerMode mode: The mode to switch to.
        """
        return await self.async_turn_on(power_mode=mode)

    async def async_start_music(self, port=0, ip=None, reconnect=False):
        """
        Start music mode.

        Music mode essentially upgrades the existing connection to a reverse one
        (the bulb connects to the library), removing all limits and allowing you
        to send commands without being rate-limited.

        Starting music mode will start a new listening socket, tell the bulb to
        connect to that, and then close the old connection. If the bulb cannot
        connect to the host machine for any reason, bad things will happen (such
        as library freezes).

        :param int port: The port to listen on. If none is specified, a random
                         port will be chosen.

        :param str ip: The IP address of the host this library is running on.
                       Will be discovered automatically if not provided.

        :param bool reconnect: Whether to reconnect to the bulb after disconnecting
        """
        async with self._music_mode_lock:
            if self._music_mode and not reconnect:
                # Music mode is enabled and we're already connected.
                raise AssertionError(
                    "Already in music mode, please stop music mode first."
                )

            if self._music_mode_state:
                _LOGGER.debug("%s: Already in music mode", self)
                return "ok"

            if reconnect:
                _LOGGER.debug("%s: Starting music mode reconnect", self)
                # Music mode is enabled but we're not connected.
                # Attempt to retrieve original parameters passed
                port, ip = self._music_mode_params or (port, ip)
                # Attempt to retrieve latest properties if on
                await self.async_get_properties(when_on=True)
            else:
                _LOGGER.debug("%s: Starting music mode", self)
                self._music_mode = True
                # Force populating the cache in case we are being called directly
                # without ever fetching properties beforehand
                await self.async_get_properties()

            future = self._loop.create_future()

            # The bulb doesn't send anything in music mode

            def on_connect(reader, writer):
                server.close()
                future.set_result((reader, writer))

            local_ip = ip if ip else self._socket.getsockname()[0]
            server = await asyncio.start_server(
                on_connect, local_ip, port, reuse_address=True
            )
            port = server.sockets[0].getsockname()[1]
            # hold the lock until we are fully enabled
            async with self._async_command_lock:
                await self.async_send_command("set_music", [1, local_ip, port])
                await self.async_stop_listening(False)
                try:
                    async with asyncio_timeout(0.5):
                        reader, writer = await asyncio.shield(future)
                except asyncio.TimeoutError:
                    # send a disconnected callback if we can't connect quickly
                    # then continue waiting to try to connect
                    _LOGGER.debug("%s: Failed to connect to music mode quickly", self)
                    if self._async_callback:
                        self._async_callback({KEY_CONNECTED: False})
                    try:
                        async with asyncio_timeout(TIMEOUT - 0.5):
                            reader, writer = await future
                    except asyncio.TimeoutError as ex:
                        # Ensures a full reconnect to the bulb
                        await self.async_stop_music(force=True)
                        raise BulbException(
                            f"Timed out enabling music mode on the bulb at {self._ip}:{self._port}."
                        ) from ex
                # Manually enable listener to watch for disconnects
                self._is_listening = True
                self._async_connected(writer, reader)
                self._music_mode_state = True
                self._listen_event = asyncio.Event()
                self._async_listen_task = asyncio.ensure_future(
                    self._async_run_listen()
                )
                try:
                    async with asyncio_timeout(TIMEOUT):
                        await self._listen_event.wait()
                except asyncio.TimeoutError:
                    # this shouldn't ever happen
                    _LOGGER.debug("%s: Listener failed to start in music mode", self)
                    await self.async_stop_music(force=True)
                    if self._async_callback:
                        self._async_callback({KEY_CONNECTED: False})
                    raise BulbException(
                        f"Timed out waiting for listener on the bulb at {self._ip}:{self._port}."
                    )
            # We are now connected
            if self._async_callback:
                self._async_callback({KEY_CONNECTED: True})

            self._music_mode_params = (port, ip)

            if reconnect:
                _LOGGER.debug("%s: Music mode reconnected successfully", self)
            else:
                _LOGGER.debug("%s: Music mode started successfully", self)

            return "ok"

    async def async_stop_music(self, force=False, **kwargs):
        """
        Stop music mode.

        Stopping music mode will close the previous connection. Calling
        ``stop_music`` more than once, or while not in music mode, is safe.
        :param bool force: Whether to force stop music mode (during exception when enabling)
        """
        self._music_mode = False

        if not self._music_mode_state and not force:
            _LOGGER.debug(
                "%s: Music mode was not enabled but async_stop_music was called",
                self,
            )
            return

        # flush out music mode socket
        self._music_expect_disconnect = True
        await self.async_stop_listening(False)
        self._music_expect_disconnect = False
        self._music_mode_state = False

        await self.async_listen(self._async_callback)
        return "set_music", [0], kwargs
