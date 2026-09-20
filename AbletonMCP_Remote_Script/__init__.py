# AbletonMCP/init.py
from __future__ import absolute_import, print_function, unicode_literals

from _Framework.ControlSurface import ControlSurface
import os
import socket
import json
import threading
import time
import traceback

# Change queue import for Python 2
try:
    import Queue as queue  # Python 2
except ImportError:
    import queue  # Python 3

# Constants for socket communication
DEFAULT_PORT = 9877
HOST = "0.0.0.0"

# Bumped whenever the TCP command surface changes; the MCP server compares
# this to EXPECTED_REMOTE_SCRIPT_VERSION.
SCRIPT_VERSION = "1.18.0"
PROTOCOL_VERSION = 1

SCRIPT_CAPABILITIES = [
    "get_session_info",
    "get_track_info",
    "get_script_info",
    "get_clip_notes",
    "get_device_parameters",
    "get_session_snapshot",
    "set_device_parameter",
    "drain_passive_events",
    "create_midi_track",
    "create_audio_track",
    "create_clip",
    "create_audio_clip",
    "add_notes_to_clip",
    "load_instrument_or_effect",
    "get_arrangement_clips",
    "duplicate_session_clip_to_arrangement",
    "create_locator",
    "delete_clip",
    "clear_notes_from_clip",
    "create_automation",
    "delete_device",
    "set_mixer_value",
    "delete_track",
    "set_audio_clip_properties",
    "create_scene",
    "delete_scene",
    "fire_scene",
    "get_groove_pool",
    "set_clip_groove",
    "investigate_render_capability",
    "get_track_routing",
    "set_track_routing",
    "undo",
    "redo",
    "set_track_state",
    "set_session_record",
    "set_color",
    "set_clip_launch_settings",
    "update_notes",
    "remove_notes_range",
    "investigate_advanced_editing",
    "add_warp_marker",
    "remove_warp_marker",
    "move_warp_marker",
    "get_project_state",
    "select_notes",
    "create_take_lane",
    "get_take_lanes",
]

def create_instance(c_instance):
    """Create and return the AbletonMCP script instance"""
    return AbletonMCP(c_instance)

class AbletonMCP(ControlSurface):
    """AbletonMCP Remote Script for Ableton Live"""
    
    def __init__(self, c_instance):
        """Initialize the control surface"""
        ControlSurface.__init__(self, c_instance)
        self.log_message(
            "AbletonMCP Remote Script initializing... (script v%s)"
            % SCRIPT_VERSION
        )
        
        # Socket server for communication
        self.server = None
        self.client_threads = []
        self.server_thread = None
        self.running = False
        
        # Cache the song reference for easier access
        self._song = self.song()

        # Passive human-UI event queue (drained by MCP → Supabase)
        self._passive_events = []
        self._passive_lock = threading.Lock()
        self._passive_max = 500
        self._passive_track_count = None
        self._passive_track_bindings = []  # (track, [(add_name, callback), ...]) for cleanup
        self._song_passive_callbacks = []
        
        # Start the socket server
        self.start_server()

        # Register LOM listeners for passive capture (after song is ready)
        try:
            self._setup_passive_listeners()
        except Exception as e:
            self.log_message("Passive listener setup failed: " + str(e))
            self.log_message(traceback.format_exc())
        
        self.log_message("AbletonMCP initialized")
        
        # Show a message in Ableton
        self.show_message("AbletonMCP: Listening for commands on port " + str(DEFAULT_PORT))
    
    def disconnect(self):
        """Called when Ableton closes or the control surface is removed"""
        self.log_message("AbletonMCP disconnecting...")
        self.running = False

        try:
            self._teardown_passive_listeners()
        except Exception as e:
            self.log_message("Passive listener teardown error: " + str(e))
        
        # Stop the server
        if self.server:
            try:
                self.server.close()
            except:
                pass
        
        # Wait for the server thread to exit
        if self.server_thread and self.server_thread.is_alive():
            self.server_thread.join(1.0)
            
        # Clean up any client threads
        for client_thread in self.client_threads[:]:
            if client_thread.is_alive():
                # We don't join them as they might be stuck
                self.log_message("Client thread still alive during disconnect")
        
        ControlSurface.disconnect(self)
        self.log_message("AbletonMCP disconnected")
    
    def start_server(self):
        """Start the socket server in a separate thread"""
        try:
            self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server.bind((HOST, DEFAULT_PORT))
            self.server.listen(5)  # Allow up to 5 pending connections
            
            self.running = True
            self.server_thread = threading.Thread(target=self._server_thread)
            self.server_thread.daemon = True
            self.server_thread.start()
            
            self.log_message("Server started on port " + str(DEFAULT_PORT))
        except Exception as e:
            self.log_message("Error starting server: " + str(e))
            self.show_message("AbletonMCP: Error starting server - " + str(e))
    
    def _server_thread(self):
        """Server thread implementation - handles client connections"""
        try:
            self.log_message("Server thread started")
            # Set a timeout to allow regular checking of running flag
            self.server.settimeout(1.0)
            
            while self.running:
                try:
                    # Accept connections with timeout
                    client, address = self.server.accept()
                    self.log_message("Connection accepted from " + str(address))
                    self.show_message("AbletonMCP: Client connected")
                    
                    # Handle client in a separate thread
                    client_thread = threading.Thread(
                        target=self._handle_client,
                        args=(client,)
                    )
                    client_thread.daemon = True
                    client_thread.start()
                    
                    # Keep track of client threads
                    self.client_threads.append(client_thread)
                    
                    # Clean up finished client threads
                    self.client_threads = [t for t in self.client_threads if t.is_alive()]
                    
                except socket.timeout:
                    # No connection yet, just continue
                    continue
                except Exception as e:
                    if self.running:  # Only log if still running
                        self.log_message("Server accept error: " + str(e))
                    time.sleep(0.5)
            
            self.log_message("Server thread stopped")
        except Exception as e:
            self.log_message("Server thread error: " + str(e))
    
    def _handle_client(self, client):
        """Handle communication with a connected client"""
        self.log_message("Client handler started")
        client.settimeout(None)  # No timeout for client socket
        buffer = ''  # Changed from b'' to '' for Python 2
        
        try:
            while self.running:
                try:
                    # Receive data
                    data = client.recv(8192)
                    
                    if not data:
                        # Client disconnected
                        self.log_message("Client disconnected")
                        break
                    
                    # Accumulate data in buffer with explicit encoding/decoding
                    try:
                        # Python 3: data is bytes, decode to string
                        buffer += data.decode('utf-8')
                    except AttributeError:
                        # Python 2: data is already string
                        buffer += data
                    
                    try:
                        # Try to parse command from buffer
                        command = json.loads(buffer)  # Removed decode('utf-8')
                        buffer = ''  # Clear buffer after successful parse
                        
                        self.log_message("Received command: " + str(command.get("type", "unknown")))
                        
                        # Process the command and get response
                        response = self._process_command(command)
                        
                        # Send the response with explicit encoding
                        try:
                            # Python 3: encode string to bytes
                            client.sendall(json.dumps(response).encode('utf-8'))
                        except AttributeError:
                            # Python 2: string is already bytes
                            client.sendall(json.dumps(response))
                    except ValueError:
                        # Incomplete data, wait for more
                        continue
                        
                except Exception as e:
                    self.log_message("Error handling client data: " + str(e))
                    self.log_message(traceback.format_exc())
                    
                    # Send error response if possible
                    error_response = {
                        "status": "error",
                        "message": str(e)
                    }
                    try:
                        # Python 3: encode string to bytes
                        client.sendall(json.dumps(error_response).encode('utf-8'))
                    except AttributeError:
                        # Python 2: string is already bytes
                        client.sendall(json.dumps(error_response))
                    except:
                        # If we can't send the error, the connection is probably dead
                        break
                    
                    # For serious errors, break the loop
                    if not isinstance(e, ValueError):
                        break
        except Exception as e:
            self.log_message("Error in client handler: " + str(e))
        finally:
            try:
                client.close()
            except:
                pass
            self.log_message("Client handler stopped")
    
    def _process_command(self, command):
        """Process a command from the client and return a response"""
        command_type = command.get("type", "")
        params = command.get("params", {})
        
        # Initialize response
        response = {
            "status": "success",
            "result": {}
        }
        
        try:
            # Route the command to the appropriate handler
            if command_type == "get_script_info":
                response["result"] = self._get_script_info()
            elif command_type == "get_session_info":
                response["result"] = self._get_session_info()
            elif command_type == "get_track_info":
                track_index = params.get("track_index", 0)
                response["result"] = self._get_track_info(track_index)
            # Commands that modify Live's state should be scheduled on the main thread
            elif command_type in ["create_midi_track", "create_audio_track", "set_track_name",
                                 "create_clip", "create_audio_clip", "add_notes_to_clip", "set_clip_name",
                                 "set_arrangement_clip_name",
                                 "delete_clip",
                                 "clear_notes_from_clip",
                                 "set_tempo", "fire_clip", "stop_clip",
                                 "start_playback", "stop_playback",
                                 "load_browser_item", "load_instrument_or_effect",
                                 # Arrangement view – must run on the main thread
                                 "switch_to_arrangement_view", "set_current_song_time",
                                 "duplicate_session_clip_to_arrangement",
                                 "map_rack_magnitude", "inspect_rack",
                                 "create_locator", "create_automation", "delete_device",
                                 "set_mixer_value", "delete_track",
                                 "set_audio_clip_properties", "create_scene",
                                 "delete_scene", "fire_scene", "set_clip_groove",
                                 "set_track_routing", "undo", "redo",
                                 "set_track_state", "set_session_record",
                                 "set_color", "set_clip_launch_settings",
                                 "update_notes", "remove_notes_range",
                                 "add_warp_marker", "remove_warp_marker",
                                 "move_warp_marker", "select_notes",
                                 "create_take_lane"]:
                # Use a thread-safe approach with a response queue
                response_queue = queue.Queue()
                
                # Define a function to execute on the main thread
                def main_thread_task():
                    try:
                        result = None
                        if command_type == "create_midi_track":
                            index = params.get("index", -1)
                            result = self._create_midi_track(index)
                        elif command_type == "create_audio_track":
                            index = params.get("index", -1)
                            result = self._create_audio_track(index)
                        elif command_type == "set_track_name":
                            track_index = params.get("track_index", 0)
                            name = params.get("name", "")
                            result = self._set_track_name(track_index, name)
                        elif command_type == "create_clip":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            length = params.get("length", 4.0)
                            result = self._create_clip(track_index, clip_index, length)
                        elif command_type == "create_audio_clip":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            path = params.get("path", "")
                            result = self._create_audio_clip(track_index, clip_index, path)
                        elif command_type == "add_notes_to_clip":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            notes = params.get("notes", [])
                            result = self._add_notes_to_clip(track_index, clip_index, notes)
                        elif command_type == "clear_notes_from_clip":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            result = self._clear_notes_from_clip(track_index, clip_index)
                        elif command_type == "set_clip_name":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            name = params.get("name", "")
                            result = self._set_clip_name(track_index, clip_index, name)
                        elif command_type == "set_arrangement_clip_name":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            name = params.get("name", "")
                            result = self._set_arrangement_clip_name(track_index, clip_index, name)
                        elif command_type == "set_tempo":
                            tempo = params.get("tempo", 120.0)
                            result = self._set_tempo(tempo)
                        elif command_type == "fire_clip":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            result = self._fire_clip(track_index, clip_index)
                        elif command_type == "stop_clip":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            result = self._stop_clip(track_index, clip_index)
                        elif command_type == "delete_clip":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            result = self._delete_clip(track_index, clip_index)
                        elif command_type == "start_playback":
                            result = self._start_playback()
                        elif command_type == "stop_playback":
                            result = self._stop_playback()
                        elif command_type == "load_instrument_or_effect":
                            track_index = params.get("track_index", 0)
                            uri = params.get("uri", "")
                            target = params.get("target", "track")
                            result = self._load_instrument_or_effect(track_index, uri, target=target)
                        elif command_type == "load_browser_item":
                            track_index = params.get("track_index", 0)
                            item_uri = params.get("item_uri", "")
                            target = params.get("target", "track")
                            result = self._load_browser_item(track_index, item_uri, target=target)
                        # ── Arrangement view commands ──────────────────────────────
                        elif command_type == "switch_to_arrangement_view":
                            result = self._switch_to_arrangement_view()
                        elif command_type == "set_current_song_time":
                            time_val = params.get("time", 0.0)
                            result = self._set_current_song_time(time_val)
                        elif command_type == "duplicate_session_clip_to_arrangement":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            destination_time = params.get("destination_time", 0.0)
                            result = self._duplicate_session_clip_to_arrangement(
                                track_index, clip_index, destination_time)
                        elif command_type == "map_rack_magnitude":
                            track_index = params.get("track_index", 0)
                            device_index = params.get("device_index", 0)
                            macro_name = params.get("macro_name", "Magnitude")
                            result = self._map_rack_magnitude(
                                track_index, device_index, macro_name)
                        elif command_type == "inspect_rack":
                            track_index = params.get("track_index", 0)
                            device_index = params.get("device_index", 0)
                            result = self._inspect_rack(track_index, device_index)
                        elif command_type == "create_locator":
                            name = params.get("name", "")
                            time_val = params.get("time", 0.0)
                            # Two-phase/async: queues its own response and may
                            # do so on a later tick, so skip the generic
                            # queue-put below for this command.
                            self._create_locator(name, time_val, response_queue)
                            return
                        elif command_type == "create_automation":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            device_index = params.get("device_index", 0)
                            parameter_index = params.get("parameter_index", 0)
                            points = params.get("points", [])
                            result = self._create_automation(
                                track_index, clip_index, device_index,
                                parameter_index, points)
                        elif command_type == "delete_device":
                            track_index = params.get("track_index", 0)
                            device_index = params.get("device_index", 0)
                            result = self._delete_device(track_index, device_index)
                        elif command_type == "set_mixer_value":
                            track_index = params.get("track_index", 0)
                            target = params.get("target", "volume")
                            value = params.get("value", 0.0)
                            send_index = params.get("send_index", None)
                            result = self._set_mixer_value(track_index, target, value, send_index)
                        elif command_type == "delete_track":
                            track_index = params.get("track_index", 0)
                            result = self._delete_track(track_index)
                        elif command_type == "set_audio_clip_properties":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            result = self._set_audio_clip_properties(
                                track_index, clip_index,
                                gain=params.get("gain", None),
                                pitch_coarse=params.get("pitch_coarse", None),
                                pitch_fine=params.get("pitch_fine", None),
                                warping=params.get("warping", None),
                            )
                        elif command_type == "create_scene":
                            index = params.get("index", -1)
                            result = self._create_scene(index)
                        elif command_type == "delete_scene":
                            index = params.get("index", 0)
                            result = self._delete_scene(index)
                        elif command_type == "fire_scene":
                            index = params.get("index", 0)
                            result = self._fire_scene(index)
                        elif command_type == "set_clip_groove":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            groove_index = params.get("groove_index", 0)
                            result = self._set_clip_groove(track_index, clip_index, groove_index)
                        elif command_type == "set_track_routing":
                            track_index = params.get("track_index", 0)
                            direction = params.get("direction", "input")
                            type_name = params.get("type_name", "")
                            result = self._set_track_routing(track_index, direction, type_name)
                        elif command_type == "undo":
                            result = self._undo()
                        elif command_type == "redo":
                            result = self._redo()
                        elif command_type == "set_track_state":
                            track_index = params.get("track_index", 0)
                            result = self._set_track_state(
                                track_index,
                                mute=params.get("mute", None),
                                solo=params.get("solo", None),
                                arm=params.get("arm", None),
                            )
                        elif command_type == "set_session_record":
                            value = params.get("value", False)
                            result = self._set_session_record(value)
                        elif command_type == "set_color":
                            result = self._set_color(
                                params.get("target", "track"),
                                params.get("color", 0),
                                track_index=params.get("track_index", None),
                                clip_index=params.get("clip_index", None),
                                scene_index=params.get("scene_index", None),
                            )
                        elif command_type == "set_clip_launch_settings":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            result = self._set_clip_launch_settings(
                                track_index, clip_index,
                                quantization=params.get("quantization", None),
                                legato=params.get("legato", None),
                                follow_action_a=params.get("follow_action_a", None),
                                follow_action_b=params.get("follow_action_b", None),
                                follow_action_time=params.get("follow_action_time", None),
                            )
                        elif command_type == "update_notes":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            notes = params.get("notes", [])
                            result = self._update_notes(track_index, clip_index, notes)
                        elif command_type == "remove_notes_range":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            result = self._remove_notes_range(
                                track_index, clip_index,
                                params.get("from_time", 0.0),
                                params.get("from_pitch", 0),
                                params.get("time_span", 128.0),
                                params.get("pitch_span", 128),
                            )
                        elif command_type == "add_warp_marker":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            result = self._add_warp_marker(
                                track_index, clip_index,
                                params.get("beat_time", 0.0),
                                params.get("sample_time", 0.0),
                            )
                        elif command_type == "remove_warp_marker":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            result = self._remove_warp_marker(
                                track_index, clip_index, params.get("beat_time", 0.0)
                            )
                        elif command_type == "move_warp_marker":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            result = self._move_warp_marker(
                                track_index, clip_index,
                                params.get("from_beat_time", 0.0),
                                params.get("to_beat_time", 0.0),
                            )
                        elif command_type == "select_notes":
                            track_index = params.get("track_index", 0)
                            clip_index = params.get("clip_index", 0)
                            result = self._select_notes(
                                track_index, clip_index,
                                note_ids=params.get("note_ids", None),
                                select_all=params.get("select_all", False),
                                deselect=params.get("deselect", False),
                            )
                        elif command_type == "create_take_lane":
                            track_index = params.get("track_index", 0)
                            result = self._create_take_lane(track_index)

                        # Put the result in the queue
                        response_queue.put({"status": "success", "result": result})
                    except Exception as e:
                        self.log_message("Error in main thread task: " + str(e))
                        self.log_message(traceback.format_exc())
                        response_queue.put({"status": "error", "message": str(e)})
                
                # Schedule the task to run on the main thread
                try:
                    self.schedule_message(0, main_thread_task)
                except AssertionError:
                    # If we're already on the main thread, execute directly
                    main_thread_task()
                
                # create_audio_clip decodes/imports the file on the main
                # thread and needs more than the default headroom.
                long_running_commands = {"create_audio_clip": 60.0}
                queue_timeout = long_running_commands.get(command_type, 10.0)
                try:
                    task_response = response_queue.get(timeout=queue_timeout)
                    if task_response.get("status") == "error":
                        response["status"] = "error"
                        response["message"] = task_response.get("message", "Unknown error")
                    else:
                        response["result"] = task_response.get("result", {})
                except queue.Empty:
                    response["status"] = "error"
                    response["message"] = "Timeout waiting for operation to complete"
            elif command_type == "get_browser_item":
                uri = params.get("uri", None)
                path = params.get("path", None)
                response["result"] = self._get_browser_item(uri, path)
            elif command_type == "get_browser_categories":
                category_type = params.get("category_type", "all")
                response["result"] = self._get_browser_categories(category_type)
            elif command_type == "get_browser_items":
                path = params.get("path", "")
                item_type = params.get("item_type", "all")
                response["result"] = self._get_browser_items(path, item_type)
            # Add the new browser commands
            elif command_type == "get_browser_tree":
                category_type = params.get("category_type", "all")
                response["result"] = self.get_browser_tree(category_type)
            elif command_type == "get_browser_items_at_path":
                path = params.get("path", "")
                response["result"] = self.get_browser_items_at_path(path)
            # Read-only arrangement command – no main-thread scheduling required
            elif command_type == "get_arrangement_clips":
                track_index = params.get("track_index", 0)
                response["result"] = self._get_arrangement_clips(track_index)
            elif command_type == "get_groove_pool":
                response["result"] = self._get_groove_pool()
            elif command_type == "investigate_render_capability":
                track_index = params.get("track_index", 0)
                response["result"] = self._investigate_render_capability(track_index)
            elif command_type == "get_track_routing":
                track_index = params.get("track_index", 0)
                response["result"] = self._get_track_routing(track_index)
            elif command_type == "get_project_state":
                response["result"] = self._get_project_state()
            elif command_type == "get_take_lanes":
                track_index = params.get("track_index", 0)
                response["result"] = self._get_take_lanes(track_index)
            elif command_type == "investigate_advanced_editing":
                track_index = params.get("track_index", 0)
                clip_index = params.get("clip_index", 0)
                response["result"] = self._investigate_advanced_editing(track_index, clip_index)
            # Dataset / state-snapshot reads
            elif command_type == "get_clip_notes":
                track_index = params.get("track_index", 0)
                clip_index = params.get("clip_index", 0)
                response["result"] = self._get_clip_notes(track_index, clip_index)
            elif command_type == "get_device_parameters":
                track_index = params.get("track_index", 0)
                device_index = params.get("device_index", 0)
                response["result"] = self._get_device_parameters(track_index, device_index)
            elif command_type == "get_session_snapshot":
                include_notes = params.get("include_notes", True)
                include_params = params.get("include_params", True)
                response["result"] = self._get_session_snapshot(
                    include_notes=include_notes,
                    include_params=include_params,
                )
            elif command_type == "drain_passive_events":
                response["result"] = self._drain_passive_events()
            elif command_type == "set_device_parameter":
                response_queue = queue.Queue()

                def main_thread_task():
                    try:
                        result = self._set_device_parameter(
                            params.get("track_index", 0),
                            params.get("device_index", 0),
                            params.get("parameter_index", 0),
                            params.get("value", 0.0),
                        )
                        response_queue.put({"status": "success", "result": result})
                    except Exception as e:
                        self.log_message("Error in main thread task: " + str(e))
                        self.log_message(traceback.format_exc())
                        response_queue.put({"status": "error", "message": str(e)})

                try:
                    self.schedule_message(0, main_thread_task)
                except AssertionError:
                    main_thread_task()

                try:
                    task_response = response_queue.get(timeout=10.0)
                    if task_response.get("status") == "error":
                        response["status"] = "error"
                        response["message"] = task_response.get("message", "Unknown error")
                    else:
                        response["result"] = task_response.get("result", {})
                except queue.Empty:
                    response["status"] = "error"
                    response["message"] = "Timeout waiting for operation to complete"
            else:
                response["status"] = "error"
                response["message"] = "Unknown command: " + command_type
        except Exception as e:
            self.log_message("Error processing command: " + str(e))
            self.log_message(traceback.format_exc())
            response["status"] = "error"
            response["message"] = str(e)
        
        return response
    
    # Command implementations

    def _get_script_info(self):
        """Handshake payload for MCP server version / capability checks."""
        return {
            "name": "AbletonMCP",
            "script_version": SCRIPT_VERSION,
            "protocol_version": PROTOCOL_VERSION,
            "port": DEFAULT_PORT,
            "capabilities": list(SCRIPT_CAPABILITIES),
            "snapshot_schema": "ableton_mcp_snapshot_v2",
            "passive_listeners": True,
        }
    
    def _safe_song_property(self, attr, cast, default):
        """Read self._song.<attr> with cast, returning default on common failures.
        Catches only narrow exceptions so genuine bugs still surface."""
        try:
            return cast(getattr(self._song, attr))
        except (AttributeError, TypeError, ValueError):
            return default

    def _get_session_info(self):
        """Get information about the current session"""
        try:
            result = {
                "tempo": self._song.tempo,
                "signature_numerator": self._song.signature_numerator,
                "signature_denominator": self._song.signature_denominator,
                "track_count": len(self._song.tracks),
                "return_track_count": len(self._song.return_tracks),
                "master_track": {
                    "name": "Master",
                    "volume": self._song.master_track.mixer_device.volume.value,
                    "panning": self._song.master_track.mixer_device.panning.value
                },
                # Read via _safe_song_property so an attribute missing on a
                # given Live version falls back to its default.
                "is_playing":        self._safe_song_property("is_playing",        bool,  False),
                "current_song_time": self._safe_song_property("current_song_time", float, 0.0),
                "song_length":       self._safe_song_property("song_length",       float, 0.0),
                "loop":              self._safe_song_property("loop",              bool,  False),
                "loop_start":        self._safe_song_property("loop_start",        float, 0.0),
                "loop_length":       self._safe_song_property("loop_length",       float, 0.0),
            }
            return result
        except Exception as e:
            self.log_message("Error getting session info: " + str(e))
            raise
    
    def _get_track_info(self, track_index):
        """Get information about a track"""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            
            track = self._song.tracks[track_index]
            
            # Get clip slots
            clip_slots = []
            for slot_index, slot in enumerate(track.clip_slots):
                clip_info = None
                if slot.has_clip:
                    clip = slot.clip
                    clip_info = {
                        "name": clip.name,
                        "length": clip.length,
                        "is_playing": clip.is_playing,
                        "is_recording": clip.is_recording
                    }
                
                clip_slots.append({
                    "index": slot_index,
                    "has_clip": slot.has_clip,
                    "clip": clip_info
                })
            
            # Get devices
            devices = []
            for device_index, device in enumerate(track.devices):
                devices.append({
                    "index": device_index,
                    "name": device.name,
                    "class_name": device.class_name,
                    "type": self._get_device_type(device)
                })
            
            result = {
                "index": track_index,
                "name": track.name,
                "is_audio_track": track.has_audio_input,
                "is_midi_track": track.has_midi_input,
                "mute": track.mute,
                "solo": track.solo,
                "arm": self._safe_arm(track),
                "volume": track.mixer_device.volume.value,
                "panning": track.mixer_device.panning.value,
                "clip_slots": clip_slots,
                "devices": devices
            }
            return result
        except Exception as e:
            self.log_message("Error getting track info: " + str(e))
            raise
    
    def _safe_arm(self, track):
        """Read track.arm, returning False for tracks that have no arm state.

        Live raises RuntimeError("Master and Return Tracks have no 'Arm'
        state!") for group tracks as well as return and main tracks. A
        `getattr(track, "arm", False)` does not guard this: the attribute
        exists, so getattr's default never applies -- reading it is what
        throws, and the error is a RuntimeError rather than an AttributeError.

        Check can_be_armed first so the common path does not rely on raising,
        and keep a narrow catch for Live versions that do not expose that
        property on every track type.
        """
        try:
            if not getattr(track, "can_be_armed", False):
                return False
            return bool(track.arm)
        except (AttributeError, RuntimeError):
            return False

    def _create_midi_track(self, index):
        """Create a new MIDI track at the specified index"""
        try:
            # Create the track
            self._song.create_midi_track(index)
            
            # Get the new track
            new_track_index = len(self._song.tracks) - 1 if index == -1 else index
            new_track = self._song.tracks[new_track_index]
            
            result = {
                "index": new_track_index,
                "name": new_track.name
            }
            return result
        except Exception as e:
            self.log_message("Error creating MIDI track: " + str(e))
            raise

    def _create_audio_track(self, index):
        """Create a new audio track at the specified index"""
        try:
            # Create the track
            self._song.create_audio_track(index)

            # Get the new track
            new_track_index = len(self._song.tracks) - 1 if index == -1 else index
            new_track = self._song.tracks[new_track_index]

            result = {
                "index": new_track_index,
                "name": new_track.name
            }
            return result
        except Exception as e:
            self.log_message("Error creating audio track: " + str(e))
            raise

    def _delete_track(self, track_index):
        """Delete a track (MIDI or audio) from the song."""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            name = self._song.tracks[track_index].name
            self._song.delete_track(track_index)
            return {"deleted": name, "track_index": track_index}
        except Exception as e:
            self.log_message("Error deleting track: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _set_audio_clip_properties(self, track_index, clip_index, gain=None,
                                    pitch_coarse=None, pitch_fine=None, warping=None):
        """Set gain/pitch/warping on an audio clip. Only provided (non-None) fields are changed."""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]
            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            clip_slot = track.clip_slots[clip_index]
            if not clip_slot.has_clip:
                raise Exception("No clip in that slot")
            clip = clip_slot.clip
            if not getattr(clip, "is_audio_clip", False):
                raise Exception("Clip is not an audio clip")

            changed = {}
            if gain is not None:
                clip.gain = float(gain)
                changed["gain"] = float(clip.gain)
            if pitch_coarse is not None:
                clip.pitch_coarse = int(pitch_coarse)
                changed["pitch_coarse"] = int(clip.pitch_coarse)
            if pitch_fine is not None:
                clip.pitch_fine = int(pitch_fine)
                changed["pitch_fine"] = int(clip.pitch_fine)
            if warping is not None:
                clip.warping = bool(warping)
                changed["warping"] = bool(clip.warping)
            return changed
        except Exception as e:
            self.log_message("Error setting audio clip properties: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _create_scene(self, index):
        """Create a new scene at the given index (-1 = end of list)."""
        try:
            self._song.create_scene(index)
            new_index = len(self._song.scenes) - 1 if index == -1 else index
            return {"index": new_index, "name": self._song.scenes[new_index].name}
        except Exception as e:
            self.log_message("Error creating scene: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _delete_scene(self, index):
        """Delete the scene at the given index."""
        try:
            if index < 0 or index >= len(self._song.scenes):
                raise IndexError("Scene index out of range")
            name = self._song.scenes[index].name
            self._song.delete_scene(index)
            return {"deleted": name, "index": index}
        except Exception as e:
            self.log_message("Error deleting scene: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _fire_scene(self, index):
        """Launch every clip in the given scene."""
        try:
            if index < 0 or index >= len(self._song.scenes):
                raise IndexError("Scene index out of range")
            self._song.scenes[index].fire()
            return {"fired": index}
        except Exception as e:
            self.log_message("Error firing scene: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _get_groove_pool(self):
        """List the grooves currently loaded in the Groove Pool."""
        try:
            grooves = self._song.groove_pool.grooves
            return {"grooves": [{"index": i, "name": g.name} for i, g in enumerate(grooves)]}
        except Exception as e:
            self.log_message("Error getting groove pool: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _set_clip_groove(self, track_index, clip_index, groove_index):
        """Assign a Groove Pool groove to a clip (real swing/timing-feel, not hand-written note offsets)."""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]
            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            clip_slot = track.clip_slots[clip_index]
            if not clip_slot.has_clip:
                raise Exception("No clip in that slot")
            clip = clip_slot.clip
            grooves = self._song.groove_pool.grooves
            if groove_index < 0 or groove_index >= len(grooves):
                raise IndexError("Groove index out of range — call get_groove_pool first")
            clip.groove = grooves[groove_index]
            return {"groove": grooves[groove_index].name}
        except Exception as e:
            self.log_message("Error setting clip groove: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _investigate_render_capability(self, track_index):
        """Read-only introspection of what freeze/render/export/bounce surface
        actually exists on this Live version — deliberately does NOT call
        anything, since an unknown render/export method could have real side
        effects (writing files, long blocking operations). Returns filtered
        dir() listings so a real implementation can be designed with
        confidence instead of guessed at.
        """
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]
            keywords = ("freeze", "render", "export", "bounce", "flatten", "consolidate")

            def filtered(obj):
                return [a for a in dir(obj) if any(k in a.lower() for k in keywords)]

            result = {
                "track_attrs": filtered(track),
                "song_attrs": filtered(self._song),
                "app_attrs": filtered(self.application()),
            }
            self.log_message("investigate_render_capability: " + str(result))
            return result
        except Exception as e:
            self.log_message("Error investigating render capability: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _get_project_state(self):
        """Read the current project's file path and whether it's ever been saved."""
        try:
            file_path = getattr(self._song, "file_path", None)
            return {
                "file_path": file_path if file_path else None,
                "has_been_saved": bool(file_path),
            }
        except Exception as e:
            self.log_message("Error getting project state: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _select_notes(self, track_index, clip_index, note_ids=None, select_all=False, deselect=False):
        """Select notes in a clip by id, select all, or deselect all —
        confirmed to exist via dir(clip) introspection
        (select_notes_by_id/select_all_notes/deselect_all_notes/
        get_selected_notes_extended)."""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]
            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            clip_slot = track.clip_slots[clip_index]
            if not clip_slot.has_clip:
                raise Exception("No clip in that slot")
            clip = clip_slot.clip

            if deselect:
                clip.deselect_all_notes()
            elif select_all:
                clip.select_all_notes()
            elif note_ids:
                clip.select_notes_by_id(list(note_ids))
            else:
                raise Exception("Specify note_ids, select_all, or deselect")

            selected = clip.get_selected_notes_extended()
            return {"selected_count": len(selected)}
        except Exception as e:
            self.log_message("Error selecting notes: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _create_take_lane(self, track_index):
        """Create a new take lane on a track — confirmed to exist via
        dir(track) introspection (create_take_lane/take_lanes)."""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]
            track.create_take_lane()
            return {"created": True, "take_lane_count": len(track.take_lanes)}
        except Exception as e:
            self.log_message("Error creating take lane: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _get_take_lanes(self, track_index):
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]
            count = len(track.take_lanes)
            names = [getattr(track.take_lanes[i], "name", "") for i in range(count)]
            return {"count": count, "names": names}
        except Exception as e:
            self.log_message("Error getting take lanes: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _get_track_routing(self, track_index):
        """Read a track's current input/output routing and the available options."""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]
            result = {}
            try:
                result["input_routing_type"] = track.input_routing_type.display_name
                result["available_input_routing_types"] = [
                    t.display_name for t in track.available_input_routing_types
                ]
            except Exception as e:
                result["input_routing_error"] = str(e)
                result["track_routing_attrs"] = [a for a in dir(track) if "rout" in a.lower()]
            try:
                result["output_routing_type"] = track.output_routing_type.display_name
                result["available_output_routing_types"] = [
                    t.display_name for t in track.available_output_routing_types
                ]
            except Exception as e:
                result["output_routing_error"] = str(e)
            return result
        except Exception as e:
            self.log_message("Error getting track routing: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _set_track_routing(self, track_index, direction, type_name):
        """Set a track's input or output routing by matching display_name.

        direction: "input" or "output". type_name: exact or substring match
        against the available_*_routing_types list (case-insensitive).
        """
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]

            if direction == "input":
                options = track.available_input_routing_types
            elif direction == "output":
                options = track.available_output_routing_types
            else:
                raise Exception("direction must be 'input' or 'output'")

            match = None
            for opt in options:
                if opt.display_name.lower() == type_name.lower():
                    match = opt
                    break
            if match is None:
                for opt in options:
                    if type_name.lower() in opt.display_name.lower():
                        match = opt
                        break
            if match is None:
                raise Exception(
                    "No routing option matching '" + type_name + "' — available: " +
                    str([o.display_name for o in options])
                )

            if direction == "input":
                track.input_routing_type = match
            else:
                track.output_routing_type = match
            return {"direction": direction, "set_to": match.display_name}
        except Exception as e:
            self.log_message("Error setting track routing: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _undo(self):
        try:
            if not self._song.can_undo:
                return {"undone": False, "reason": "Nothing to undo"}
            self._song.undo()
            return {"undone": True}
        except Exception as e:
            self.log_message("Error undoing: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _redo(self):
        try:
            if not self._song.can_redo:
                return {"redone": False, "reason": "Nothing to redo"}
            self._song.redo()
            return {"redone": True}
        except Exception as e:
            self.log_message("Error redoing: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _set_track_state(self, track_index, mute=None, solo=None, arm=None):
        """Set mute/solo/arm on a track. Only provided (non-None) fields change."""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]
            changed = {}
            if mute is not None:
                track.mute = bool(mute)
                changed["mute"] = bool(track.mute)
            if solo is not None:
                track.solo = bool(solo)
                changed["solo"] = bool(track.solo)
            if arm is not None:
                if not getattr(track, "can_be_armed", False):
                    raise Exception("Track cannot be armed (return/master/group track)")
                track.arm = bool(arm)
                changed["arm"] = bool(track.arm)
            return changed
        except Exception as e:
            self.log_message("Error setting track state: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _set_session_record(self, value):
        """Set global session record.

        Deliberately does not read the value back to confirm — session_record
        is transport-level state, same class of property as current_song_time,
        and reading it back within the same tick reflects the *previous*
        state, not this call's write (confirmed live: every read was one call
        behind). Trust the write, as with any fire-and-forget transport toggle.
        """
        try:
            self._song.session_record = bool(value)
            return {"session_record_set_to": bool(value)}
        except Exception as e:
            self.log_message("Error setting session_record: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _set_color(self, target, color, track_index=None, clip_index=None, scene_index=None):
        """target: 'track', 'clip', or 'scene'. color: integer RGB, e.g. 0xFF3366."""
        try:
            color = int(color)
            if target == "track":
                if track_index is None or track_index < 0 or track_index >= len(self._song.tracks):
                    raise IndexError("Track index out of range")
                obj = self._song.tracks[track_index]
            elif target == "clip":
                if track_index is None or track_index < 0 or track_index >= len(self._song.tracks):
                    raise IndexError("Track index out of range")
                track = self._song.tracks[track_index]
                if clip_index is None or clip_index < 0 or clip_index >= len(track.clip_slots):
                    raise IndexError("Clip index out of range")
                clip_slot = track.clip_slots[clip_index]
                if not clip_slot.has_clip:
                    raise Exception("No clip in that slot")
                obj = clip_slot.clip
            elif target == "scene":
                if scene_index is None or scene_index < 0 or scene_index >= len(self._song.scenes):
                    raise IndexError("Scene index out of range")
                obj = self._song.scenes[scene_index]
            else:
                raise Exception("target must be 'track', 'clip', or 'scene'")
            obj.color = color
            return {"target": target, "color": int(obj.color)}
        except Exception as e:
            self.log_message("Error setting color: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _set_clip_launch_settings(self, track_index, clip_index, quantization=None,
                                   legato=None, follow_action_a=None,
                                   follow_action_b=None, follow_action_time=None):
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]
            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            clip_slot = track.clip_slots[clip_index]
            if not clip_slot.has_clip:
                raise Exception("No clip in that slot")
            clip = clip_slot.clip
            changed = {}
            if quantization is not None:
                clip.launch_quantization = int(quantization)
                changed["launch_quantization"] = int(clip.launch_quantization)
            if legato is not None:
                clip.legato = bool(legato)
                changed["legato"] = bool(clip.legato)
            if follow_action_a is not None:
                clip.follow_action_a = int(follow_action_a)
                changed["follow_action_a"] = int(clip.follow_action_a)
            if follow_action_b is not None:
                clip.follow_action_b = int(follow_action_b)
                changed["follow_action_b"] = int(clip.follow_action_b)
            if follow_action_time is not None:
                clip.follow_action_time = float(follow_action_time)
                changed["follow_action_time"] = float(clip.follow_action_time)
            if not changed:
                changed["clip_launch_attrs"] = [a for a in dir(clip) if "launch" in a.lower() or "follow" in a.lower() or "legato" in a.lower()]
            return changed
        except Exception as e:
            self.log_message("Error setting clip launch settings: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _update_notes(self, track_index, clip_index, notes):
        """Modify specific existing notes in place by note_id, without
        touching any other notes in the clip — unlike add_notes_to_clip
        (append-only) or clear_notes_from_clip (all-or-nothing), this is a
        real targeted edit. Uses Clip.apply_note_modifications, the Live 11+
        API for this; each note dict must include the note_id returned by
        get_clip_notes.
        """
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]
            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            clip_slot = track.clip_slots[clip_index]
            if not clip_slot.has_clip:
                raise Exception("No clip in that slot")
            clip = clip_slot.clip

            if not hasattr(clip, "apply_note_modifications") or not hasattr(clip, "get_notes_extended"):
                return {
                    "applied": False,
                    "error": "apply_note_modifications/get_notes_extended not available on this Live version",
                    "clip_note_attrs": [a for a in dir(clip) if "note" in a.lower()],
                }

            # apply_note_modifications binds to a fixed C++ struct (TNoteInfo)
            # and — confirmed live, across four attempts — will NEVER accept
            # a Python-constructed list or tuple, no matter what's inside it
            # (a plain dict, a real Clip.MidiNote, mutated or untouched).
            # The real fix, confirmed live: it only accepts its own native
            # container type, Clip.MidiNoteVector — specifically, a SLICE of
            # the vector returned by get_notes_extended. So: keep that
            # native vector intact (never call list() on it), mutate the
            # target notes' attributes in place by indexing into it, then
            # hand back a full-range slice of the SAME vector.
            raw_notes = clip.get_notes_extended(0, 128, 0.0, float(clip.length) + 1.0)
            note_count = len(raw_notes)

            by_id = {}
            for i in range(note_count):
                n = raw_notes[i]
                nid = getattr(n, "note_id", None)
                if nid is not None:
                    by_id[int(nid)] = n

            missing = []
            modified_count = 0
            for spec in notes:
                nid = int(spec["note_id"])
                note_obj = by_id.get(nid)
                if note_obj is None:
                    missing.append(nid)
                    continue
                if "pitch" in spec:
                    note_obj.pitch = int(spec["pitch"])
                if "start_time" in spec:
                    note_obj.start_time = float(spec["start_time"])
                if "duration" in spec:
                    note_obj.duration = float(spec["duration"])
                if "velocity" in spec:
                    note_obj.velocity = float(spec["velocity"])
                if "mute" in spec:
                    note_obj.mute = bool(spec["mute"])
                if "probability" in spec and hasattr(note_obj, "probability"):
                    note_obj.probability = float(spec["probability"])
                if "velocity_deviation" in spec and hasattr(note_obj, "velocity_deviation"):
                    note_obj.velocity_deviation = float(spec["velocity_deviation"])
                if "release_velocity" in spec and hasattr(note_obj, "release_velocity"):
                    note_obj.release_velocity = float(spec["release_velocity"])
                modified_count += 1

            if modified_count == 0:
                return {"applied": False, "error": "No matching note_ids found", "missing_note_ids": missing}

            clip.apply_note_modifications(raw_notes[0:note_count])
            result = {"applied": True, "count": modified_count}
            if missing:
                result["missing_note_ids"] = missing
            return result
        except Exception as e:
            self.log_message("Error updating notes: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _remove_notes_range(self, track_index, clip_index, from_time, from_pitch, time_span, pitch_span):
        """Remove only the notes within a time/pitch range, without clearing
        the whole clip. Uses Clip.remove_notes_extended (Live 11+)."""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]
            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            clip_slot = track.clip_slots[clip_index]
            if not clip_slot.has_clip:
                raise Exception("No clip in that slot")
            clip = clip_slot.clip

            if not hasattr(clip, "remove_notes_extended"):
                return {
                    "removed": False,
                    "error": "clip.remove_notes_extended not available on this Live version",
                    "clip_note_attrs": [a for a in dir(clip) if "note" in a.lower()],
                }

            # Real C++ signature (confirmed live via the boost::python type
            # error): (from_pitch: int, pitch_span: int, from_time: double,
            # time_span: double) — pitch args first and grouped together,
            # not interleaved with time as the more "readable" order would
            # suggest.
            clip.remove_notes_extended(
                int(from_pitch), int(pitch_span), float(from_time), float(time_span)
            )
            return {"removed": True}
        except Exception as e:
            self.log_message("Error removing notes range: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _get_warp_clip(self, track_index, clip_index):
        if track_index < 0 or track_index >= len(self._song.tracks):
            raise IndexError("Track index out of range")
        track = self._song.tracks[track_index]
        if clip_index < 0 or clip_index >= len(track.clip_slots):
            raise IndexError("Clip index out of range")
        clip_slot = track.clip_slots[clip_index]
        if not clip_slot.has_clip:
            raise Exception("No clip in that slot")
        clip = clip_slot.clip
        if not getattr(clip, "is_audio_clip", False):
            raise Exception("Warp markers only apply to audio clips")
        return clip

    def _add_warp_marker(self, track_index, clip_index, beat_time, sample_time):
        """Add a warp marker to an audio clip.

        Confirmed live: neither a dict nor a tuple works — the real error
        is "No registered converter ... type NApiHelpers::TWarpMarker from
        ... type tuple", the exact same class of problem as MIDI notes
        (a genuine C++ struct, no generic-object conversion registered).
        Live.Clip module lists a WarpMarker class directly, so trying to
        construct one properly instead of guessing another bare container.
        """
        WarpMarker = None
        try:
            clip = self._get_warp_clip(track_index, clip_index)
            import Live.Clip as _live_clip_mod
            WarpMarker = _live_clip_mod.WarpMarker
            try:
                marker = WarpMarker(float(beat_time), float(sample_time))
            except Exception as e1:
                self.log_message("add_warp_marker: positional WarpMarker() failed (" + str(e1) + "), trying kwargs")
                marker = WarpMarker(beat_time=float(beat_time), sample_time=float(sample_time))
            clip.add_warp_marker(marker)
            return {"added": True, "beat_time": float(beat_time), "sample_time": float(sample_time)}
        except Exception as e:
            if WarpMarker is not None:
                self.log_message("add_warp_marker: WarpMarker class dir=" + str([a for a in dir(WarpMarker) if not a.startswith("_")]))
            self.log_message("Error adding warp marker: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _find_nearest_warp_marker_index(self, clip, beat_time):
        """Find the index of the nearest warp marker WITHOUT converting the
        native WarpMarkerVector to a plain list — list() is exactly what
        broke the equivalent MIDI-note lookup (it silently discards the
        native-container type information the write-side API needs back)."""
        count = len(clip.warp_markers)
        if count == 0:
            raise Exception("Clip has no warp markers")
        best_i, best_diff = 0, None
        for i in range(count):
            diff = abs(getattr(clip.warp_markers[i], "beat_time", 0.0) - float(beat_time))
            if best_diff is None or diff < best_diff:
                best_i, best_diff = i, diff
        return best_i

    def _remove_warp_marker(self, track_index, clip_index, beat_time):
        """Remove the warp marker nearest to beat_time, passing a native
        slice of clip.warp_markers back — the same pattern proven to work
        for MIDI notes (a slice of the native vector, never a Python list)."""
        try:
            clip = self._get_warp_clip(track_index, clip_index)
            i = self._find_nearest_warp_marker_index(clip, beat_time)
            marker_beat_time = clip.warp_markers[i].beat_time
            try:
                clip.remove_warp_marker(clip.warp_markers[i:i + 1])
            except Exception as e1:
                self.log_message("remove_warp_marker: vector-slice form failed (" + str(e1) + "), trying single-element indexing")
                clip.remove_warp_marker(clip.warp_markers[i])
            return {"removed_near_beat_time": marker_beat_time}
        except Exception as e:
            self.log_message("Error removing warp marker: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _move_warp_marker(self, track_index, clip_index, from_beat_time, to_beat_time):
        """Move an existing warp marker: mutate its beat_time in place
        (matching how MIDI note attributes were mutated) via native
        indexing into clip.warp_markers, then hand back a native slice."""
        try:
            clip = self._get_warp_clip(track_index, clip_index)
            i = self._find_nearest_warp_marker_index(clip, from_beat_time)
            original_time = clip.warp_markers[i].beat_time
            clip.warp_markers[i].beat_time = float(to_beat_time)
            try:
                clip.move_warp_marker(clip.warp_markers[i:i + 1])
            except Exception as e1:
                self.log_message("move_warp_marker: apply-after-mutate failed (" + str(e1) + ") — mutation itself may already be sufficient without a separate apply call")
            return {"moved_from": original_time, "moved_to": float(to_beat_time)}
        except Exception as e:
            self.log_message("Error moving warp marker: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _investigate_advanced_editing(self, track_index, clip_index):
        """Read-only introspection for the genuinely uncertain items: warp
        marker editing, mid-arrangement time signature changes, track
        reordering, and Simpler/Sampler slice/reverse control. Deliberately
        does not guess at calling any of these — just surfaces the real
        dir() so a follow-up pass can implement with confidence instead of
        trial-and-error against a live project.
        """
        result = {}
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]

            # Warp markers
            if clip_index is not None and 0 <= clip_index < len(track.clip_slots):
                clip_slot = track.clip_slots[clip_index]
                if clip_slot.has_clip:
                    clip = clip_slot.clip
                    result["clip_warp_attrs"] = [a for a in dir(clip) if "warp" in a.lower()]
                else:
                    result["clip_warp_attrs"] = "no clip in that slot"

            # Time signature (global vs per-position)
            result["song_signature_attrs"] = [a for a in dir(self._song) if "signature" in a.lower() or "time_sig" in a.lower()]

            # Track reordering — "move" as a naive substring also matches
            # every "remove_*_listener" method (re-MOVE contains "move"),
            # which drowned out the real signal on the first pass. Exclude
            # anything starting with "add_"/"remove_" (those are always
            # listener (de)registration in this API, never reordering).
            def reorder_candidates(obj):
                return [a for a in dir(obj)
                        if not a.startswith(("add_", "remove_"))
                        and ("reorder" in a.lower() or "move" in a.lower() or "position" in a.lower() or "index" in a.lower())]
            result["song_track_order_attrs"] = reorder_candidates(self._song)
            result["track_order_attrs"] = reorder_candidates(track)

            # Simpler/Sampler slicing — check devices on this track for
            # anything that looks like a sampler
            slicing_info = []
            for d_i, device in enumerate(track.devices):
                cname = getattr(device, "class_name", "")
                if "sampl" in cname.lower() or "simpler" in device.name.lower() or "sampler" in device.name.lower():
                    slicing_info.append({
                        "device_index": d_i,
                        "name": device.name,
                        "slice_attrs": [a for a in dir(device) if "slic" in a.lower() or "revers" in a.lower() or "sample" in a.lower()],
                    })
            result["sampler_devices"] = slicing_info

            # Project file path / unsaved-changes state
            app = self.application()
            result["app_attrs_re_file"] = [a for a in dir(app) if any(k in a.lower() for k in ("file", "path", "document", "dirty", "modified", "saved"))]
            result["song_attrs_re_file"] = [a for a in dir(self._song) if any(k in a.lower() for k in ("file", "path", "document", "dirty", "modified", "saved"))]
            # Open/new project — expected to be structurally absent (a
            # Remote Script lives inside one already-open document), same
            # reasoning as freeze/render. Checking rather than assuming.
            result["app_attrs_re_project"] = [a for a in dir(app) if any(k in a.lower() for k in ("open_", "new_", "load_project", "close_"))]

            # Note selection state (as opposed to editing by note_id)
            if clip_index is not None and 0 <= clip_index < len(track.clip_slots):
                clip_slot = track.clip_slots[clip_index]
                if clip_slot.has_clip:
                    clip = clip_slot.clip
                    result["clip_select_attrs"] = [a for a in dir(clip) if "select" in a.lower()]
                    if hasattr(clip, "view"):
                        result["clip_view_attrs"] = [a for a in dir(clip.view) if not a.startswith("_")]

            # Take-lane / comping — Song showed a take_lanes-related listener
            # in an earlier (differently-filtered) pass; confirming directly.
            result["track_take_lane_attrs"] = [a for a in dir(track) if "take_lane" in a.lower() or "comp" in a.lower()]
            if hasattr(track, "take_lanes"):
                try:
                    lanes = list(track.take_lanes)
                    result["take_lanes_count"] = len(lanes)
                    if lanes:
                        result["take_lane_0_attrs"] = [a for a in dir(lanes[0]) if not a.startswith("_")]
                except Exception as e:
                    result["take_lanes_error"] = str(e)

            self.log_message("investigate_advanced_editing: " + str(result))
            return result
        except Exception as e:
            self.log_message("Error investigating advanced editing: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _set_track_name(self, track_index, name):
        """Set the name of a track"""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            
            # Set the name
            track = self._song.tracks[track_index]
            track.name = name
            
            result = {
                "name": track.name
            }
            return result
        except Exception as e:
            self.log_message("Error setting track name: " + str(e))
            raise
    
    def _create_clip(self, track_index, clip_index, length):
        """Create a new MIDI clip in the specified track and clip slot"""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            
            track = self._song.tracks[track_index]
            
            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            
            clip_slot = track.clip_slots[clip_index]
            
            # Check if the clip slot already has a clip
            if clip_slot.has_clip:
                raise Exception("Clip slot already has a clip")
            
            # Create the clip
            clip_slot.create_clip(length)
            
            result = {
                "name": clip_slot.clip.name,
                "length": clip_slot.clip.length
            }
            return result
        except Exception as e:
            self.log_message("Error creating clip: " + str(e))
            raise

    def _create_audio_clip(self, track_index, clip_index, path):
        """Create an audio clip in the specified audio track clip slot by importing a file.

        Requires Ableton Live 12.0.5 or newer (the underlying
        ClipSlot.create_audio_clip Live API was introduced in 12.0.5 — it is
        not available in earlier 12.0.x releases).
        """
        try:
            if not path:
                raise ValueError("Audio file path is required")

            if not os.path.isabs(path):
                raise ValueError("Audio file path must be absolute (got: %s)" % path)

            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")

            track = self._song.tracks[track_index]

            if getattr(track, "has_midi_input", False) or not getattr(track, "has_audio_input", True):
                raise ValueError("Track %d is not an audio track" % track_index)

            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")

            clip_slot = track.clip_slots[clip_index]

            if clip_slot.has_clip:
                raise Exception("Clip slot already has a clip")

            if not hasattr(clip_slot, "create_audio_clip"):
                raise Exception(
                    "ClipSlot.create_audio_clip is unavailable in this Ableton Live "
                    "version. Requires Live 12.0.5 or newer."
                )

            clip_slot.create_audio_clip(path)

            result = {
                "name": clip_slot.clip.name,
                "length": clip_slot.clip.length,
                "is_audio_clip": clip_slot.clip.is_audio_clip
            }
            return result
        except Exception as e:
            self.log_message("Error creating audio clip: " + str(e))
            raise

    def _add_notes_to_clip(self, track_index, clip_index, notes):
        """Add MIDI notes to a clip"""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            
            track = self._song.tracks[track_index]
            
            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            
            clip_slot = track.clip_slots[clip_index]
            
            if not clip_slot.has_clip:
                raise Exception("No clip in slot")
            
            clip = clip_slot.clip
            
            # Convert note data to Live's format
            live_notes = []
            for note in notes:
                pitch = note.get("pitch", 60)
                start_time = note.get("start_time", 0.0)
                duration = note.get("duration", 0.25)
                velocity = note.get("velocity", 100)
                mute = note.get("mute", False)
                
                live_notes.append((pitch, start_time, duration, velocity, mute))
            
            # Add the notes
            clip.set_notes(tuple(live_notes))
            
            result = {
                "note_count": len(notes)
            }
            return result
        except Exception as e:
            self.log_message("Error adding notes to clip: " + str(e))
            raise
    
    def _set_clip_name(self, track_index, clip_index, name):
        """Set the name of a clip"""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            
            track = self._song.tracks[track_index]
            
            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            
            clip_slot = track.clip_slots[clip_index]
            
            if not clip_slot.has_clip:
                raise Exception("No clip in slot")
            
            clip = clip_slot.clip
            clip.name = name
            
            result = {
                "name": clip.name
            }
            return result
        except Exception as e:
            self.log_message("Error setting clip name: " + str(e))
            raise

    def _set_arrangement_clip_name(self, track_index, clip_index, name):
        """Set the name of a clip placed in the Arrangement timeline.

        clip_index indexes into track.arrangement_clips, in the same order
        as returned by _get_arrangement_clips (i.e. ordered by start_time).
        """
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")

            track = self._song.tracks[track_index]
            arrangement_clips = list(track.arrangement_clips)

            if clip_index < 0 or clip_index >= len(arrangement_clips):
                raise IndexError("Clip index out of range")

            clip = arrangement_clips[clip_index]
            clip.name = name

            result = {
                "name": clip.name
            }
            return result
        except Exception as e:
            self.log_message("Error setting arrangement clip name: " + str(e))
            raise

    def _set_tempo(self, tempo):
        """Set the tempo of the session"""
        try:
            self._song.tempo = tempo
            
            result = {
                "tempo": self._song.tempo
            }
            return result
        except Exception as e:
            self.log_message("Error setting tempo: " + str(e))
            raise
    
    def _fire_clip(self, track_index, clip_index):
        """Fire a clip"""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            
            track = self._song.tracks[track_index]
            
            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            
            clip_slot = track.clip_slots[clip_index]
            
            if not clip_slot.has_clip:
                raise Exception("No clip in slot")
            
            clip_slot.fire()
            
            result = {
                "fired": True
            }
            return result
        except Exception as e:
            self.log_message("Error firing clip: " + str(e))
            raise
    
    def _stop_clip(self, track_index, clip_index):
        """Stop a clip"""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            
            track = self._song.tracks[track_index]
            
            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            
            clip_slot = track.clip_slots[clip_index]
            
            clip_slot.stop()
            
            result = {
                "stopped": True
            }
            return result
        except Exception as e:
            self.log_message("Error stopping clip: " + str(e))
            raise

    def _delete_clip(self, track_index, clip_index):
        """Delete the clip in the given clip slot, freeing the slot for reuse."""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")

            track = self._song.tracks[track_index]

            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")

            clip_slot = track.clip_slots[clip_index]

            if not clip_slot.has_clip:
                return {"deleted": False, "reason": "Clip slot was already empty"}

            clip_slot.delete_clip()

            return {"deleted": True}
        except Exception as e:
            self.log_message("Error deleting clip: " + str(e))
            raise


    def _start_playback(self):
        """Start playing the session"""
        try:
            self._song.start_playing()
            
            result = {
                "playing": self._song.is_playing
            }
            return result
        except Exception as e:
            self.log_message("Error starting playback: " + str(e))
            raise
    
    def _stop_playback(self):
        """Stop playing the session"""
        try:
            self._song.stop_playing()
            
            result = {
                "playing": self._song.is_playing
            }
            return result
        except Exception as e:
            self.log_message("Error stopping playback: " + str(e))
            raise
    
    # ── Arrangement view implementations ──────────────────────────────────────

    def _switch_to_arrangement_view(self):
        """Switch Ableton's main window to the Arrangement view"""
        try:
            self.application().view.show_view("Arranger")
            return {"view": "Arranger"}
        except Exception as e:
            self.log_message("Error switching to arrangement view: " + str(e))
            raise

    def _set_current_song_time(self, time_val):
        """Move the arrangement playhead to a position in beats"""
        try:
            self._song.current_song_time = float(time_val)
            return {"current_song_time": self._song.current_song_time}
        except Exception as e:
            self.log_message("Error setting current song time: " + str(e))
            raise

    def _get_arrangement_clips(self, track_index):
        """Return all clips placed in the Arrangement timeline for a track.

        Each clip dict contains:
          name, start_time, end_time, length, color,
          is_midi_clip, is_audio_clip, is_playing
        """
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")

            track = self._song.tracks[track_index]
            clips = []

            # track.arrangement_clips is available in Live 11 / 12
            for clip in track.arrangement_clips:
                clips.append({
                    "name": clip.name,
                    "start_time": clip.start_time,
                    "end_time": clip.end_time,
                    "length": clip.length,
                    "color": clip.color,
                    "is_midi_clip": clip.is_midi_clip,
                    "is_audio_clip": clip.is_audio_clip,
                    "is_playing": clip.is_playing
                })

            return {
                "track_index": track_index,
                "track_name": track.name,
                "clip_count": len(clips),
                "clips": clips
            }
        except Exception as e:
            self.log_message("Error getting arrangement clips: " + str(e))
            raise

    def _clear_notes_from_clip(self, track_index, clip_index):
        """Remove all MIDI notes from a Session clip.

        Pairs with _add_notes_to_clip to make a real replace (clear, then add),
        which the write-only API otherwise can't do. Counts notes first so the
        result can report how many were removed.
        """
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")

            track = self._song.tracks[track_index]

            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")

            clip_slot = track.clip_slots[clip_index]

            if not clip_slot.has_clip:
                raise Exception("No clip in slot")

            clip = clip_slot.clip

            if not clip.is_midi_clip:
                raise Exception("Clip is not a MIDI clip; no notes to clear")

            length = clip.length

            # Count existing notes for the report (best-effort; never fatal).
            cleared = 0
            try:
                getter = getattr(clip, "get_notes_extended", None)
                if getter is not None:
                    cleared = len(list(getter(0, 128, 0.0, length)))
                else:
                    cleared = len(list(clip.get_notes(0.0, 0, length, 128)))
            except Exception:
                cleared = 0

            # Remove every note across the full pitch/time range. Prefer the
            # modern API (Live 11+); fall back to the legacy signature. Argument
            # order mirrors the get/remove _extended family:
            #   remove_notes_extended(from_pitch, pitch_span, from_time, time_span)
            # vs the legacy remove_notes(from_time, from_pitch, time_span, pitch_span).
            remover = getattr(clip, "remove_notes_extended", None)
            if remover is not None:
                remover(0, 128, 0.0, length)
            else:
                clip.remove_notes(0.0, 0, length, 128)

            return {
                "track_index": track_index,
                "clip_index": clip_index,
                "clip_name": clip.name,
                "cleared_count": cleared,
            }
        except Exception as e:
            self.log_message("Error clearing notes from clip: " + str(e))
            raise

    def _duplicate_session_clip_to_arrangement(self, track_index, clip_index, destination_time):
        """Copy a Session-view clip into the Arrangement timeline.

        Uses the real Live API:
          track.duplicate_clip_to_arrangement(clip, destination_time)

        Available in Live 11 / 12.  destination_time is in beats from the
        start of the arrangement.
        """
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")

            track = self._song.tracks[track_index]

            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip slot index out of range")

            clip_slot = track.clip_slots[clip_index]

            if not clip_slot.has_clip:
                raise Exception(
                    "No clip in slot " + str(clip_index) +
                    " on track " + str(track_index)
                )

            clip = clip_slot.clip

            # Duplicate to arrangement at the requested beat position
            track.duplicate_clip_to_arrangement(clip, float(destination_time))

            return {
                "success": True,
                "track_index": track_index,
                "track_name": track.name,
                "clip_name": clip.name,
                "destination_time": destination_time
            }
        except Exception as e:
            self.log_message("Error duplicating clip to arrangement: " + str(e))
            raise

    def _create_locator(self, name, time_val, response_queue):
        """Create (or rename) a named locator at the given beat position.

        Two-phase and asynchronous — puts its own result onto response_queue
        rather than returning a value, because a single main-thread tick is
        not enough: setting song.current_song_time and immediately reading
        it back (or calling set_or_delete_cue() right after) does not
        reliably reflect the change within that same tick (observed: the
        read-back stayed at the old position every time). Phase 1 sets the
        position and schedules phase 2 one tick later; phase 2 does the
        toggle-and-verify once the engine has actually caught up.

        Renaming an already-existing cue needs no position change, so that
        case is still handled synchronously.
        """
        try:
            song = self._song
            target_time = float(time_val)
            tolerance = 1e-3

            for cue in song.cue_points:
                if abs(cue.time - target_time) < tolerance:
                    if name:
                        try:
                            cue.name = str(name)
                        except Exception as e:
                            self.log_message("Could not rename locator: " + str(e))
                    response_queue.put({"status": "success", "result": {
                        "time": cue.time, "name": cue.name, "renamed": True,
                    }})
                    return

            original_time = song.current_song_time

            def phase2():
                try:
                    song.set_or_delete_cue()
                    existing = None
                    for cue in song.cue_points:
                        if abs(cue.time - target_time) < tolerance:
                            existing = cue
                            break
                    try:
                        song.current_song_time = original_time
                    except Exception:
                        pass

                    if existing is None:
                        self.log_message(
                            "create_locator phase2 diagnostic: target=" + str(target_time) +
                            " current_song_time_now=" + str(song.current_song_time) +
                            " all_cue_times=" + str([c.time for c in song.cue_points])
                        )
                        response_queue.put({"status": "error", "message":
                            "Failed to create cue at time " + str(target_time) +
                            " even after the two-phase timing fix"})
                        return

                    if name:
                        try:
                            existing.name = str(name)
                        except Exception as e:
                            self.log_message("Could not rename locator: " + str(e))

                    response_queue.put({"status": "success", "result": {
                        "time": existing.time, "name": existing.name, "renamed": False,
                    }})
                except Exception as e:
                    self.log_message("Error in create_locator phase2: " + str(e))
                    self.log_message(traceback.format_exc())
                    response_queue.put({"status": "error", "message": str(e)})

            song.current_song_time = target_time
            self.schedule_message(1, phase2)
        except Exception as e:
            self.log_message("Error creating locator (phase1): " + str(e))
            self.log_message(traceback.format_exc())
            response_queue.put({"status": "error", "message": str(e)})

    def _delete_device(self, track_index, device_index):
        """Delete a device from a track's device chain."""
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]
            if device_index < 0 or device_index >= len(track.devices):
                raise IndexError("Device index out of range")
            name = track.devices[device_index].name
            track.delete_device(device_index)
            return {"deleted": name, "device_index": device_index}
        except Exception as e:
            self.log_message("Error deleting device: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    def _create_automation(self, track_index, clip_index, device_index, parameter_index, points):
        """Write an automation envelope for a device parameter into a Session clip.

        points: list of {"time": beats_from_clip_start, "value": native_param_value},
        at least 2 points, sorted by time. The LOM's envelope API is step-based
        (insert_step) rather than true curves, so a ramp between two points is
        approximated as many short constant-value steps (one per 16th note,
        capped at 64 substeps per segment).

        Best-effort implementation: the exact AutomationEnvelope method surface
        isn't confirmed against this Live version yet. If insert_step's
        signature doesn't match, this logs the real object's available methods
        (dir(envelope)) to Live's Log.txt so the call can be corrected in one
        follow-up pass instead of guessing blind.
        """
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]

            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            clip_slot = track.clip_slots[clip_index]
            if not clip_slot.has_clip:
                raise Exception("No clip in that slot to attach automation to")
            clip = clip_slot.clip

            if device_index < 0 or device_index >= len(track.devices):
                raise IndexError("Device index out of range")
            device = track.devices[device_index]

            if parameter_index < 0 or parameter_index >= len(device.parameters):
                raise IndexError("Parameter index out of range")
            param = device.parameters[parameter_index]

            if not points or len(points) < 2:
                raise Exception("Need at least 2 points (time, value) to write an envelope")

            self.log_message(
                "create_automation: param=" + str(param.name) +
                " is_enabled=" + str(getattr(param, "is_enabled", "?")) +
                " automation_state=" + str(getattr(param, "automation_state", "?"))
            )

            envelope = clip.automation_envelope(param)
            if envelope is None:
                # No envelope exists yet for this parameter on this clip —
                # automation_envelope() only reads, create_automation_envelope()
                # actually creates one.
                envelope = clip.create_automation_envelope(param)
            if envelope is None:
                raise Exception(
                    "clip.create_automation_envelope(param) also returned None for "
                    "parameter '" + str(param.name) + "' — parameter may not be "
                    "automatable at all."
                )

            self.log_message(
                "create_automation: envelope acquired, available attrs: " +
                str([a for a in dir(envelope) if not a.startswith("_")])
            )

            try:
                envelope.clear_envelope()
            except Exception as e:
                self.log_message("create_automation: clear_envelope failed (continuing): " + str(e))

            points_sorted = sorted(points, key=lambda p: float(p["time"]))
            inserted = 0
            last_error = None

            for i in range(len(points_sorted) - 1):
                t0 = float(points_sorted[i]["time"])
                v0 = float(points_sorted[i]["value"])
                t1 = float(points_sorted[i + 1]["time"])
                v1 = float(points_sorted[i + 1]["value"])
                span = t1 - t0
                if span <= 0:
                    continue
                substeps = max(1, min(64, int(span / 0.25)))
                step_len = span / substeps
                for s in range(substeps):
                    t = t0 + s * step_len
                    frac = s / float(substeps)
                    v = v0 + (v1 - v0) * frac
                    try:
                        envelope.insert_step(t, step_len, v)
                        inserted += 1
                    except Exception as e:
                        last_error = str(e)
                        break
                if last_error:
                    break

            result = {"inserted_steps": inserted, "parameter": param.name}
            if last_error:
                result["error"] = last_error
                result["envelope_attrs"] = [a for a in dir(envelope) if not a.startswith("_")]
                self.log_message("create_automation: insert_step failed: " + last_error)
            return result
        except Exception as e:
            self.log_message("Error creating automation: " + str(e))
            self.log_message(traceback.format_exc())
            raise

    # ── Browser implementations ───────────────────────────────────────────────

    def _get_browser_item(self, uri, path):
        """Get a browser item by URI or path"""
        try:
            # Access the application's browser instance instead of creating a new one
            app = self.application()
            if not app:
                raise RuntimeError("Could not access Live application")
                
            result = {
                "uri": uri,
                "path": path,
                "found": False
            }
            
            # Try to find by URI first if provided
            if uri:
                item = self._find_browser_item_by_uri(app.browser, uri)
                if item:
                    result["found"] = True
                    result["item"] = {
                        "name": item.name,
                        "is_folder": item.is_folder,
                        "is_device": item.is_device,
                        "is_loadable": item.is_loadable,
                        "uri": item.uri
                    }
                    return result
            
            # If URI not provided or not found, try by path
            if path:
                # Parse the path and navigate to the specified item
                path_parts = path.split("/")
                
                # Determine the root based on the first part
                current_item = None
                if path_parts[0].lower() == "instruments":
                    current_item = app.browser.instruments
                elif path_parts[0].lower() == "sounds":
                    current_item = app.browser.sounds
                elif path_parts[0].lower() == "drums":
                    current_item = app.browser.drums
                elif path_parts[0].lower() == "audio_effects":
                    current_item = app.browser.audio_effects
                elif path_parts[0].lower() == "midi_effects":
                    current_item = app.browser.midi_effects
                else:
                    # Default to instruments if not specified
                    current_item = app.browser.instruments
                    # Don't skip the first part in this case
                    path_parts = ["instruments"] + path_parts
                
                # Navigate through the path
                for i in range(1, len(path_parts)):
                    part = path_parts[i]
                    if not part:  # Skip empty parts
                        continue
                    
                    found = False
                    part_lower = part.lower()
                    for child in current_item.children:
                        if not hasattr(child, 'name'):
                            continue
                        child_name_lower = child.name.lower()
                        if (child_name_lower == part_lower or
                                child_name_lower == part_lower + ".adg" or
                                os.path.splitext(child_name_lower)[0] == part_lower):
                            current_item = child
                            found = True
                            break

                    if not found:
                        result["error"] = "Path part '{0}' not found".format(part)
                        result["available_children"] = [c.name for c in current_item.children if hasattr(c, 'name')]
                        return result
                
                # Found the item
                result["found"] = True
                result["item"] = {
                    "name": current_item.name,
                    "is_folder": current_item.is_folder,
                    "is_device": current_item.is_device,
                    "is_loadable": current_item.is_loadable,
                    "uri": current_item.uri
                }
            
            return result
        except Exception as e:
            self.log_message("Error getting browser item: " + str(e))
            self.log_message(traceback.format_exc())
            raise   
    
    
    
    def _load_instrument_or_effect(self, track_index, uri, target="track"):
        """Load an instrument or effect onto a track by its browser URI.

        The command dispatcher above calls this method, but it was never
        defined — and "load_instrument_or_effect" was missing from the list of
        main-thread commands as well, so the command fell through to the final
        "Unknown command" branch. Loading a device is exactly what
        _load_browser_item does, so delegate to it; the only difference is the
        parameter name the MCP server uses ("uri" vs "item_uri").
        """
        return self._load_browser_item(track_index, uri, target=target)

    def _resolve_track_for_loading(self, track_index, target):
        """target: 'track' (default, song.tracks), 'return', or 'master'."""
        if target == "master":
            return self._song.master_track
        if target == "return":
            if track_index < 0 or track_index >= len(self._song.return_tracks):
                raise IndexError("Return track index out of range")
            return self._song.return_tracks[track_index]
        if track_index < 0 or track_index >= len(self._song.tracks):
            raise IndexError("Track index out of range")
        return self._song.tracks[track_index]

    def _load_browser_item(self, track_index, item_uri, target="track"):
        """Load a browser item onto a track (or a return/master track) by URI."""
        try:
            track = self._resolve_track_for_loading(track_index, target)
            
            # Access the application's browser instance instead of creating a new one
            app = self.application()
            
            # Find the browser item by URI
            item = self._find_browser_item_by_uri(app.browser, item_uri)
            
            if not item:
                raise ValueError("Browser item with URI '{0}' not found".format(item_uri))
            
            # Select the track
            self._song.view.selected_track = track
            
            # Load the item
            app.browser.load_item(item)
            
            result = {
                "loaded": True,
                "item_name": item.name,
                "track_name": track.name,
                "uri": item_uri
            }
            return result
        except Exception as e:
            self.log_message("Error loading browser item: {0}".format(str(e)))
            self.log_message(traceback.format_exc())
            raise
    
    # Substring markers that point a URI at a likely root. Unmatched URIs fall
    # back to the default search order.
    _URI_ROOT_HINTS = (
        ('plugins',       ('vst:', 'vst3:', 'au:', 'query:plugins', 'plugin#')),
        ('max_for_live',  ('max for live', 'maxforlive', 'm4l', 'query:max')),
        ('user_library',  ('user library', 'userlibrary', 'query:user library', 'query:user-library')),
        ('packs',         ('query:packs', '/packs/')),
        ('samples',       ('query:samples', 'sample:', '/samples/')),
        ('drums',         ('query:drums', '/drums/')),
        ('instruments',   ('query:instruments', '/instruments/')),
        ('sounds',        ('query:sounds', '/sounds/')),
        ('audio_effects', ('query:audio effects', 'audioeffects', '/audio_effects/')),
        ('midi_effects',  ('query:midi effects', 'midieffects', '/midi_effects/')),
    )

    def _order_roots_by_uri(self, roots, uri):
        """Reorder ``roots`` so the URI's likely root is walked first."""
        if not isinstance(uri, (bytes, str)) or not uri:
            return roots
        lowered = uri.lower()
        for attr, markers in self._URI_ROOT_HINTS:
            if any(m in lowered for m in markers):
                head = [(a, r) for (a, r) in roots if a == attr]
                tail = [(a, r) for (a, r) in roots if a != attr]
                return head + tail
        return roots

    def _find_browser_item_by_uri(self, browser_or_item, uri, max_depth=10, current_depth=0):
        """Find a browser item by its URI.

        Top-level lookups are memoised on ``self._uri_cache`` so repeated
        loads of the same URI don't re-walk the entire browser tree.
        """
        if current_depth == 0:
            cache = getattr(self, '_uri_cache', None)
            if cache is None:
                self._uri_cache = cache = {}
            if uri in cache:
                return cache[uri]
            result = self._walk_browser_for_uri(browser_or_item, uri, max_depth, 0)
            if result is not None:
                cache[uri] = result
            return result
        return self._walk_browser_for_uri(browser_or_item, uri, max_depth, current_depth)

    def _walk_browser_for_uri(self, browser_or_item, uri, max_depth, current_depth):
        """Recursive walk used by :py:meth:`_find_browser_item_by_uri`."""
        try:
            # Check if this is the item we're looking for
            if hasattr(browser_or_item, 'uri') and browser_or_item.uri == uri:
                return browser_or_item

            # Stop recursion if we've reached max depth
            if current_depth >= max_depth:
                return None

            # Check if this is a browser with root categories
            if hasattr(browser_or_item, 'instruments'):
                roots = [
                    ('instruments', browser_or_item.instruments),
                    ('sounds', browser_or_item.sounds),
                    ('drums', browser_or_item.drums),
                    ('audio_effects', browser_or_item.audio_effects),
                    ('midi_effects', browser_or_item.midi_effects),
                ]
                for extra_attr in ('plugins', 'max_for_live', 'user_library', 'packs', 'samples'):
                    if hasattr(browser_or_item, extra_attr):
                        try:
                            roots.append((extra_attr, getattr(browser_or_item, extra_attr)))
                        except (AttributeError, RuntimeError) as e:
                            self.log_message("Could not access browser.{0}: {1}".format(extra_attr, str(e)))

                for _attr, category in self._order_roots_by_uri(roots, uri):
                    item = self._find_browser_item_by_uri(category, uri, max_depth, current_depth + 1)
                    if item:
                        return item

                return None

            # Check if this item has children
            if hasattr(browser_or_item, 'children') and browser_or_item.children:
                for child in browser_or_item.children:
                    item = self._find_browser_item_by_uri(child, uri, max_depth, current_depth + 1)
                    if item:
                        return item

            return None
        except Exception as e:
            self.log_message("Error finding browser item by URI: {0}".format(str(e)))
            return None
    
    # Helper methods

    def _find_blend_parameter(self, device):
        """Find Dry/Wet, Mix, or Amount on a device for Magnitude mapping."""
        preferred = ("Dry/Wet", "Dry Wet", "Mix", "Amount")
        by_name = {}
        for param in device.parameters:
            try:
                by_name[param.name] = param
            except Exception:
                continue
        for name in preferred:
            if name in by_name:
                return by_name[name], name
        # Case-insensitive fallback
        lowered = dict((k.lower(), (v, k)) for k, v in by_name.items())
        for name in preferred:
            hit = lowered.get(name.lower())
            if hit:
                return hit[0], hit[1]
        return None, None

    def _inspect_rack(self, track_index, device_index=0):
        """Inspect a rack's nested devices and blend parameters."""
        if track_index < 0 or track_index >= len(self._song.tracks):
            raise IndexError("Track index out of range")
        track = self._song.tracks[track_index]
        if device_index < 0 or device_index >= len(track.devices):
            raise IndexError("Device index out of range")
        rack = track.devices[device_index]
        if not getattr(rack, "can_have_chains", False):
            raise ValueError("Device '{0}' is not a rack".format(rack.name))

        devices_info = []
        for chain_index, chain in enumerate(rack.chains):
            for nested in chain.devices:
                blend, blend_name = self._find_blend_parameter(nested)
                param_names = []
                try:
                    param_names = [p.name for p in nested.parameters]
                except Exception:
                    pass
                devices_info.append({
                    "chain_index": chain_index,
                    "name": nested.name,
                    "class_name": nested.class_name,
                    "blend_param": blend_name,
                    "parameters": param_names,
                })

        return {
            "track_index": track_index,
            "device_index": device_index,
            "rack_name": rack.name,
            "has_macro_map": hasattr(rack, "macro_map"),
            "has_rename_macro": hasattr(rack, "rename_macro"),
            "macros_mapped": list(getattr(rack, "macros_mapped", [])),
            "devices": devices_info,
        }

    def _map_rack_magnitude(self, track_index, device_index=0, macro_name="Magnitude"):
        """Rename Macro 1 and map nested Dry/Wet (or Mix/Amount) params to it."""
        if track_index < 0 or track_index >= len(self._song.tracks):
            raise IndexError("Track index out of range")
        track = self._song.tracks[track_index]
        if device_index < 0 or device_index >= len(track.devices):
            raise IndexError("Device index out of range")
        rack = track.devices[device_index]
        if not getattr(rack, "can_have_chains", False):
            raise ValueError("Device '{0}' is not a rack".format(rack.name))
        if not hasattr(rack, "macro_map"):
            raise RuntimeError(
                "RackDevice.macro_map is unavailable in this Live version")

        # Ensure at least one macro is visible
        try:
            visible = int(getattr(rack, "visible_macro_count", 1) or 1)
            while visible < 1 and hasattr(rack, "add_macro"):
                rack.add_macro()
                visible = int(rack.visible_macro_count)
        except Exception as e:
            self.log_message("Could not adjust visible macros: {0}".format(e))

        if hasattr(rack, "rename_macro"):
            rack.rename_macro(0, macro_name)
        else:
            # Fallback: Macro 1 is usually parameters[1] (0 = Device On)
            try:
                if len(rack.parameters) > 1:
                    rack.parameters[1].name = macro_name
            except Exception:
                pass

        mapped = []
        skipped = []
        for chain_index, chain in enumerate(rack.chains):
            for nested in chain.devices:
                blend, blend_name = self._find_blend_parameter(nested)
                if not blend:
                    skipped.append({
                        "device": nested.name,
                        "reason": "no Dry/Wet, Mix, or Amount parameter",
                    })
                    continue
                try:
                    rack.macro_map(0, blend)
                    mapped.append({
                        "device": nested.name,
                        "parameter": blend_name,
                        "chain_index": chain_index,
                    })
                except Exception as e:
                    skipped.append({
                        "device": nested.name,
                        "parameter": blend_name,
                        "reason": str(e),
                    })

        return {
            "rack_name": rack.name,
            "macro_name": macro_name,
            "macro_index": 0,
            "mapped": mapped,
            "skipped": skipped,
            "macros_mapped": list(getattr(rack, "macros_mapped", [])),
        }
    
    def _get_device_type(self, device):
        """Get the type of a device"""
        try:
            # Simple heuristic - in a real implementation you'd look at the device class
            if device.can_have_drum_pads:
                return "drum_machine"
            elif device.can_have_chains:
                return "rack"
            elif "instrument" in device.class_display_name.lower():
                return "instrument"
            elif "audio_effect" in device.class_name.lower():
                return "audio_effect"
            elif "midi_effect" in device.class_name.lower():
                return "midi_effect"
            else:
                return "unknown"
        except:
            return "unknown"

    # ── Passive human-UI listeners ──────────────────────────────────────────────

    def _enqueue_passive(self, event_type, detail=None, track_index=None, clip_index=None):
        """Append a coarse human-UI event (capped FIFO)."""
        evt = {
            "type": event_type,
            "ts": time.time(),
            "track_index": track_index,
            "clip_index": clip_index,
            "detail": detail if detail is not None else {},
        }
        with self._passive_lock:
            self._passive_events.append(evt)
            if len(self._passive_events) > self._passive_max:
                self._passive_events = self._passive_events[-self._passive_max:]

    def _drain_passive_events(self):
        """Return and clear the passive event queue (called by MCP poller)."""
        with self._passive_lock:
            events = list(self._passive_events)
            self._passive_events = []
        return {"events": events, "count": len(events)}

    def _safe_add_listener(self, obj, add_name, callback):
        try:
            if obj is not None and hasattr(obj, add_name):
                getattr(obj, add_name)(callback)
                return True
        except Exception as e:
            self.log_message("add listener %s failed: %s" % (add_name, str(e)))
        return False

    def _safe_remove_listener(self, obj, remove_name, callback):
        try:
            if obj is not None and hasattr(obj, remove_name):
                getattr(obj, remove_name)(callback)
        except Exception:
            pass

    def _setup_passive_listeners(self):
        """Register high-signal LOM listeners for Mode C / assisted human edits."""
        song = self._song

        def on_tempo():
            try:
                self._enqueue_passive("tempo_changed", {"tempo": float(song.tempo)})
            except Exception:
                self._enqueue_passive("tempo_changed")

        def on_sig_num():
            try:
                self._enqueue_passive(
                    "time_signature_changed",
                    {
                        "signature_numerator": int(song.signature_numerator),
                        "signature_denominator": int(song.signature_denominator),
                    },
                )
            except Exception:
                self._enqueue_passive("time_signature_changed")

        def on_sig_den():
            on_sig_num()

        def on_is_playing():
            try:
                self._enqueue_passive(
                    "playback_changed",
                    {"is_playing": bool(song.is_playing)},
                )
            except Exception:
                self._enqueue_passive("playback_changed")

        def on_tracks():
            count = len(song.tracks)
            previous = getattr(self, "_passive_track_count", None)
            self._passive_track_count = count
            detail = {"track_count": count}
            if previous is not None:
                detail["previous_count"] = previous
                detail["removed"] = count < previous
                detail["added"] = count > previous
            self._enqueue_passive("tracks_changed", detail)
            try:
                self._rebind_track_listeners()
            except Exception as e:
                self.log_message("rebind track listeners failed: " + str(e))

        self._safe_add_listener(song, "add_tempo_listener", on_tempo)
        self._safe_add_listener(song, "add_signature_numerator_listener", on_sig_num)
        self._safe_add_listener(song, "add_signature_denominator_listener", on_sig_den)
        self._safe_add_listener(song, "add_is_playing_listener", on_is_playing)
        self._safe_add_listener(song, "add_tracks_listener", on_tracks)

        self._song_passive_callbacks = [
            ("tempo_listener", on_tempo),
            ("signature_numerator_listener", on_sig_num),
            ("signature_denominator_listener", on_sig_den),
            ("is_playing_listener", on_is_playing),
            ("tracks_listener", on_tracks),
        ]

        self._rebind_track_listeners()
        self.log_message("Passive LOM listeners registered")

    def _teardown_passive_listeners(self):
        song = getattr(self, "_song", None)
        for suffix, cb in getattr(self, "_song_passive_callbacks", []):
            self._safe_remove_listener(song, "remove_" + suffix, cb)
        self._clear_track_listeners()

    def _clear_track_listeners(self):
        for track, bindings in getattr(self, "_passive_track_bindings", []):
            for add_name, callback in bindings:
                remove_name = "remove_" + add_name[len("add_"):]
                target = getattr(callback, "_passive_target", None)
                if target is not None:
                    self._safe_remove_listener(target, remove_name, callback)
                else:
                    self._safe_remove_listener(track, remove_name, callback)
        self._passive_track_bindings = []

    def _rebind_track_listeners(self):
        self._clear_track_listeners()
        song = self._song
        for track_index, track in enumerate(song.tracks):
            bindings = []

            def make_track_cb(kind, t_index):
                def _cb():
                    detail = {}
                    try:
                        t = song.tracks[t_index]
                        if kind == "name_changed":
                            detail["name"] = t.name
                        elif kind == "mute_changed":
                            detail["mute"] = bool(t.mute)
                        elif kind == "solo_changed":
                            detail["solo"] = bool(t.solo)
                        elif kind == "arm_changed":
                            detail["arm"] = self._safe_arm(t)
                        elif kind == "devices_changed":
                            detail["device_count"] = len(t.devices)
                        elif kind == "volume_changed":
                            detail["volume"] = float(t.mixer_device.volume.value)
                        elif kind == "panning_changed":
                            detail["panning"] = float(t.mixer_device.panning.value)
                    except Exception:
                        pass
                    self._enqueue_passive(kind, detail, track_index=t_index)
                return _cb

            pairs = [
                ("add_name_listener", "name_changed"),
                ("add_mute_listener", "mute_changed"),
                ("add_solo_listener", "solo_changed"),
                ("add_arm_listener", "arm_changed"),
                ("add_devices_listener", "devices_changed"),
            ]
            for add_name, kind in pairs:
                cb = make_track_cb(kind, track_index)
                if self._safe_add_listener(track, add_name, cb):
                    bindings.append((add_name, cb))

            try:
                mixer = track.mixer_device
                vol_cb = make_track_cb("volume_changed", track_index)
                vol_cb._passive_target = mixer.volume
                if self._safe_add_listener(mixer.volume, "add_value_listener", vol_cb):
                    bindings.append(("add_value_listener", vol_cb))
                pan_cb = make_track_cb("panning_changed", track_index)
                pan_cb._passive_target = mixer.panning
                if self._safe_add_listener(mixer.panning, "add_value_listener", pan_cb):
                    bindings.append(("add_value_listener", pan_cb))
            except Exception as e:
                self.log_message("mixer listeners failed on track %d: %s" % (track_index, str(e)))

            try:
                for clip_index, slot in enumerate(track.clip_slots):
                    def make_slot_cb(t_index, c_index):
                        def _cb():
                            has = False
                            try:
                                has = bool(song.tracks[t_index].clip_slots[c_index].has_clip)
                            except Exception:
                                pass
                            self._enqueue_passive(
                                "clip_slot_changed",
                                {"has_clip": has},
                                track_index=t_index,
                                clip_index=c_index,
                            )
                            try:
                                self._bind_clip_listeners(t_index, c_index)
                            except Exception:
                                pass
                        return _cb

                    slot_cb = make_slot_cb(track_index, clip_index)
                    slot_cb._passive_target = slot
                    if self._safe_add_listener(slot, "add_has_clip_listener", slot_cb):
                        bindings.append(("add_has_clip_listener", slot_cb))
                    if slot.has_clip:
                        self._bind_clip_listeners(track_index, clip_index, bindings)
            except Exception as e:
                self.log_message("clip slot listeners failed on track %d: %s" % (track_index, str(e)))

            self._passive_track_bindings.append((track, bindings))

    def _bind_clip_listeners(self, track_index, clip_index, bindings=None):
        try:
            track = self._song.tracks[track_index]
            slot = track.clip_slots[clip_index]
            if not slot.has_clip:
                return
            clip = slot.clip

            def on_name():
                name = ""
                try:
                    name = clip.name
                except Exception:
                    pass
                self._enqueue_passive(
                    "clip_name_changed",
                    {"name": name},
                    track_index=track_index,
                    clip_index=clip_index,
                )

            def on_notes():
                self._enqueue_passive(
                    "clip_notes_changed",
                    {},
                    track_index=track_index,
                    clip_index=clip_index,
                )

            def on_playing():
                playing = False
                try:
                    playing = bool(clip.is_playing)
                except Exception:
                    pass
                self._enqueue_passive(
                    "clip_playing_changed",
                    {"is_playing": playing},
                    track_index=track_index,
                    clip_index=clip_index,
                )

            for add_name, cb in [
                ("add_name_listener", on_name),
                ("add_notes_listener", on_notes),
                ("add_playing_status_listener", on_playing),
            ]:
                cb._passive_target = clip
                if self._safe_add_listener(clip, add_name, cb):
                    if bindings is not None:
                        bindings.append((add_name, cb))
        except Exception as e:
            self.log_message(
                "bind clip listeners %d/%d failed: %s"
                % (track_index, clip_index, str(e))
            )

    # ── Dataset / state snapshot helpers ──────────────────────────────────────

    def _safe_attr(self, obj, attr, cast=None, default=None):
        try:
            val = getattr(obj, attr)
            if callable(val):
                return default
            if cast is not None:
                return cast(val)
            return val
        except Exception:
            return default

    def _notes_from_clip(self, clip):
        """Extract MIDI notes from a clip (incl. MPE/expression when available)."""
        notes = []
        if not clip or not getattr(clip, "is_midi_clip", False):
            return notes

        if hasattr(clip, "get_notes_extended"):
            try:
                raw = clip.get_notes_extended(0, 128, 0.0, float(clip.length) + 1.0)
                for n in raw:
                    entry = {
                        "pitch": int(getattr(n, "pitch", 0)),
                        "start_time": float(getattr(n, "start_time", 0.0)),
                        "duration": float(getattr(n, "duration", 0.0)),
                        "velocity": float(getattr(n, "velocity", 0)),
                        "mute": bool(getattr(n, "mute", False)),
                    }
                    for opt, caster in [
                        ("probability", float),
                        ("velocity_deviation", float),
                        ("release_velocity", float),
                        ("note_id", int),
                    ]:
                        if hasattr(n, opt):
                            try:
                                entry[opt] = caster(getattr(n, opt))
                            except Exception:
                                pass
                    for opt in ("pitch_bend_range", "pressure", "timbre", "slide"):
                        if hasattr(n, opt):
                            try:
                                entry[opt] = float(getattr(n, opt))
                            except Exception:
                                pass
                    notes.append(entry)
                return notes
            except Exception as e:
                self.log_message("get_notes_extended failed, falling back: " + str(e))

        if hasattr(clip, "get_notes"):
            try:
                raw = clip.get_notes(0.0, 0, float(clip.length) + 1.0, 128)
                for n in raw:
                    notes.append({
                        "pitch": int(n[0]),
                        "start_time": float(n[1]),
                        "duration": float(n[2]),
                        "velocity": float(n[3]),
                        "mute": bool(n[4]) if len(n) > 4 else False,
                    })
            except Exception as e:
                self.log_message("get_notes failed: " + str(e))
        return notes

    def _warp_markers_from_clip(self, clip):
        markers = []
        try:
            raw = getattr(clip, "warp_markers", None)
            if not raw:
                return markers
            for m in raw:
                markers.append({
                    "beat_time": float(getattr(m, "beat_time", getattr(m, "time", 0.0))),
                    "sample_time": float(
                        getattr(m, "sample_time", getattr(m, "time", 0.0))
                    ),
                })
        except Exception as e:
            self.log_message("warp_markers read failed: " + str(e))
        return markers

    def _automated_params_for_device(self, device):
        automated = []
        try:
            for param in device.parameters:
                is_auto = False
                try:
                    if hasattr(param, "automation_state"):
                        is_auto = int(param.automation_state) != 0
                    elif hasattr(param, "is_automated"):
                        is_auto = bool(param.is_automated)
                except Exception:
                    continue
                if is_auto:
                    automated.append(param.name)
        except Exception:
            pass
        return automated

    # Racks nest, and a pathological project could nest deeply. Cap the walk so
    # a snapshot can never blow the stack or the payload size.
    _MAX_CHAIN_DEPTH = 4

    def _serialize_device(self, device, device_index, include_params=True, depth=0):
        info = {
            "index": device_index,
            "name": device.name,
            "class_name": device.class_name,
            "type": self._get_device_type(device),
        }
        automated = self._automated_params_for_device(device)
        if automated:
            info["automated_parameters"] = automated
            info["automation_enabled"] = True
        else:
            info["automation_enabled"] = False

        if include_params:
            params = []
            try:
                for p_index, param in enumerate(device.parameters):
                    try:
                        entry = {
                            "index": p_index,
                            "name": param.name,
                            "value": float(param.value),
                            "min": float(param.min),
                            "max": float(param.max),
                            "is_enabled": bool(getattr(param, "is_enabled", True)),
                            "is_quantized": bool(getattr(param, "is_quantized", False)),
                        }
                        if hasattr(param, "value_string"):
                            entry["value_string"] = str(param.value_string)
                        if hasattr(param, "automation_state"):
                            try:
                                entry["automation_state"] = int(param.automation_state)
                            except Exception:
                                pass
                        params.append(entry)
                    except Exception:
                        continue
            except Exception as e:
                self.log_message("Error reading device parameters: " + str(e))
            info["parameters"] = params

        # Devices inside a rack carry the actual sound design — a drum rack's
        # nested Operator, an instrument rack's filter. Without this walk a rack
        # contributes only its 8 macros and the timbral state is invisible.
        if getattr(device, "can_have_chains", False):
            if depth >= self._MAX_CHAIN_DEPTH:
                info["chains_truncated"] = True
            else:
                info["chains"] = self._serialize_chains(
                    device, include_params=include_params, depth=depth
                )
        return info

    def _serialize_chains(self, rack, include_params=True, depth=0):
        chains = []
        try:
            chain_lists = [("chains", getattr(rack, "chains", []))]
            returns = getattr(rack, "return_chains", None)
            if returns:
                chain_lists.append(("return_chains", returns))

            for kind, chain_list in chain_lists:
                for chain_index, chain in enumerate(chain_list):
                    entry = {
                        "index": chain_index,
                        "kind": kind,
                        "chain_name": self._safe_attr(chain, "name", str, ""),
                        "mute": bool(self._safe_attr(chain, "mute", bool, False)),
                        "solo": bool(self._safe_attr(chain, "solo", bool, False)),
                    }
                    try:
                        mixer = chain.mixer_device
                        entry["volume"] = float(mixer.volume.value)
                        entry["panning"] = float(mixer.panning.value)
                    except Exception:
                        pass

                    # Drum racks expose the pad's note, which is what ties a
                    # nested device back to the kick/snare/hat it voices.
                    note = self._safe_attr(chain, "out_note", int, None)
                    if note is not None:
                        entry["out_note"] = note

                    nested = []
                    try:
                        for d_i, dev in enumerate(chain.devices):
                            nested.append(
                                self._serialize_device(
                                    dev,
                                    d_i,
                                    include_params=include_params,
                                    depth=depth + 1,
                                )
                            )
                    except Exception as e:
                        self.log_message("Error reading chain devices: " + str(e))
                    entry["devices"] = nested
                    chains.append(entry)
        except Exception as e:
            self.log_message("Error serializing rack chains: " + str(e))
        return chains

    def _serialize_clip_common(self, clip):
        info = {
            "looping": bool(self._safe_attr(clip, "looping", bool, False)),
            "loop_start": self._safe_attr(clip, "loop_start", float, None),
            "loop_end": self._safe_attr(clip, "loop_end", float, None),
            "warping": bool(self._safe_attr(clip, "warping", bool, False)),
            "warp_mode": self._safe_attr(clip, "warp_mode", int, None),
            "gain": self._safe_attr(clip, "gain", float, None),
            "pitch_coarse": self._safe_attr(clip, "pitch_coarse", int, None),
            "pitch_fine": self._safe_attr(clip, "pitch_fine", int, None),
            "launch_mode": self._safe_attr(clip, "launch_mode", int, None),
        }
        for attr in ("file_path", "file_path_relative"):
            path = self._safe_attr(clip, attr, str, None)
            if path:
                info["file_path"] = path
                break
        markers = self._warp_markers_from_clip(clip)
        if markers:
            info["warp_markers"] = markers
            info["warp_marker_count"] = len(markers)
        return dict((k, v) for k, v in info.items() if v is not None)

    def _serialize_session_clip(self, clip, include_notes=True):
        info = {
            "name": clip.name,
            "length": float(clip.length),
            "is_playing": bool(clip.is_playing),
            "is_recording": bool(getattr(clip, "is_recording", False)),
            "is_midi_clip": bool(getattr(clip, "is_midi_clip", False)),
            "is_audio_clip": bool(getattr(clip, "is_audio_clip", False)),
            "color": int(getattr(clip, "color", 0)),
        }
        info.update(self._serialize_clip_common(clip))
        if include_notes and info["is_midi_clip"]:
            info["notes"] = self._notes_from_clip(clip)
            info["note_count"] = len(info["notes"])
        return info

    def _serialize_arrangement_clip(self, clip, include_notes=True):
        info = {
            "name": clip.name,
            "start_time": float(clip.start_time),
            "end_time": float(clip.end_time),
            "length": float(clip.length),
            "color": int(getattr(clip, "color", 0)),
            "is_midi_clip": bool(getattr(clip, "is_midi_clip", False)),
            "is_audio_clip": bool(getattr(clip, "is_audio_clip", False)),
            "is_playing": bool(getattr(clip, "is_playing", False)),
        }
        info.update(self._serialize_clip_common(clip))
        if include_notes and info["is_midi_clip"]:
            info["notes"] = self._notes_from_clip(clip)
            info["note_count"] = len(info["notes"])
        return info

    def _serialize_sends(self, track):
        sends = []
        try:
            for i, send in enumerate(track.mixer_device.sends):
                sends.append({
                    "index": i,
                    "value": float(send.value),
                    "name": str(getattr(send, "name", "Send %d" % i)),
                })
        except Exception:
            pass
        return sends

    def _serialize_scenes(self):
        scenes = []
        try:
            for i, scene in enumerate(self._song.scenes):
                scenes.append({
                    "index": i,
                    "name": str(scene.name),
                    "tempo": self._safe_attr(scene, "tempo", float, None),
                    "is_triggered": bool(self._safe_attr(scene, "is_triggered", bool, False)),
                })
        except Exception as e:
            self.log_message("scenes serialize failed: " + str(e))
        return scenes

    def _serialize_cue_points(self):
        cues = []
        try:
            for cue in self._song.cue_points:
                cues.append({
                    "name": str(getattr(cue, "name", "")),
                    "time": float(getattr(cue, "time", 0.0)),
                })
        except Exception as e:
            self.log_message("cue_points serialize failed: " + str(e))
        return cues

    def _serialize_return_tracks(self, include_params=True):
        returns = []
        try:
            for i, track in enumerate(self._song.return_tracks):
                devices = []
                for d_i, device in enumerate(track.devices):
                    devices.append(
                        self._serialize_device(device, d_i, include_params=include_params)
                    )
                returns.append({
                    "index": i,
                    "name": track.name,
                    "mute": bool(track.mute),
                    "solo": bool(track.solo),
                    "volume": float(track.mixer_device.volume.value),
                    "panning": float(track.mixer_device.panning.value),
                    "devices": devices,
                })
        except Exception as e:
            self.log_message("return_tracks serialize failed: " + str(e))
        return returns

    def _serialize_master_track(self, include_params=True):
        """Master chain — the bus compressor/limiter that shapes the final sound."""
        try:
            track = self._song.master_track
            devices = []
            for d_i, device in enumerate(track.devices):
                devices.append(
                    self._serialize_device(device, d_i, include_params=include_params)
                )
            return {
                "volume": float(track.mixer_device.volume.value),
                "panning": float(track.mixer_device.panning.value),
                "devices": devices,
            }
        except Exception as e:
            self.log_message("master_track serialize failed: " + str(e))
            return None

    def _get_clip_notes(self, track_index, clip_index):
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]
            if clip_index < 0 or clip_index >= len(track.clip_slots):
                raise IndexError("Clip index out of range")
            slot = track.clip_slots[clip_index]
            if not slot.has_clip:
                raise Exception("No clip in slot")
            clip = slot.clip
            if not getattr(clip, "is_midi_clip", False):
                raise Exception("Clip is not a MIDI clip")
            notes = self._notes_from_clip(clip)
            return {
                "track_index": track_index,
                "clip_index": clip_index,
                "clip_name": clip.name,
                "length": float(clip.length),
                "note_count": len(notes),
                "notes": notes,
            }
        except Exception as e:
            self.log_message("Error getting clip notes: " + str(e))
            raise

    def _get_device_parameters(self, track_index, device_index):
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]
            if device_index < 0 or device_index >= len(track.devices):
                raise IndexError("Device index out of range")
            device = track.devices[device_index]
            return {
                "track_index": track_index,
                "device": self._serialize_device(device, device_index, include_params=True),
            }
        except Exception as e:
            self.log_message("Error getting device parameters: " + str(e))
            raise

    def _get_session_snapshot(self, include_notes=True, include_params=True):
        """Full v2 project state dump for trajectory dataset recording."""
        try:
            session = self._get_session_info()
            tracks = []
            for track_index, track in enumerate(self._song.tracks):
                clip_slots = []
                for slot_index, slot in enumerate(track.clip_slots):
                    clip_info = None
                    if slot.has_clip:
                        clip_info = self._serialize_session_clip(
                            slot.clip, include_notes=include_notes
                        )
                    clip_slots.append({
                        "index": slot_index,
                        "has_clip": bool(slot.has_clip),
                        "clip": clip_info,
                    })

                devices = []
                for device_index, device in enumerate(track.devices):
                    devices.append(
                        self._serialize_device(
                            device, device_index, include_params=include_params
                        )
                    )

                arrangement_clips = []
                try:
                    for clip in track.arrangement_clips:
                        arrangement_clips.append(
                            self._serialize_arrangement_clip(
                                clip, include_notes=include_notes
                            )
                        )
                except Exception as e:
                    self.log_message(
                        "arrangement_clips unavailable on track %d: %s"
                        % (track_index, str(e))
                    )

                tracks.append({
                    "index": track_index,
                    "name": track.name,
                    "is_audio_track": bool(track.has_audio_input),
                    "is_midi_track": bool(track.has_midi_input),
                    "mute": bool(track.mute),
                    "solo": bool(track.solo),
                    "arm": self._safe_arm(track),
                    "volume": float(track.mixer_device.volume.value),
                    "panning": float(track.mixer_device.panning.value),
                    "sends": self._serialize_sends(track),
                    "clip_slots": clip_slots,
                    "devices": devices,
                    "arrangement_clips": arrangement_clips,
                })

            return {
                "schema": "ableton_mcp_snapshot_v2",
                "session": session,
                "tracks": tracks,
                "scenes": self._serialize_scenes(),
                "return_tracks": self._serialize_return_tracks(
                    include_params=include_params
                ),
                "master_track": self._serialize_master_track(
                    include_params=include_params
                ),
                "cue_points": self._serialize_cue_points(),
                "include_notes": bool(include_notes),
                "include_params": bool(include_params),
            }
        except Exception as e:
            self.log_message("Error getting session snapshot: " + str(e))
            raise

    def _set_device_parameter(self, track_index, device_index, parameter_index, value):
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]
            if device_index < 0 or device_index >= len(track.devices):
                raise IndexError("Device index out of range")
            device = track.devices[device_index]
            if parameter_index < 0 or parameter_index >= len(device.parameters):
                raise IndexError("Parameter index out of range")
            param = device.parameters[parameter_index]
            old = float(param.value)
            param.value = float(value)
            return {
                "track_index": track_index,
                "device_index": device_index,
                "parameter_index": parameter_index,
                "name": param.name,
                "old_value": old,
                "value": float(param.value),
                "min": float(param.min),
                "max": float(param.max),
            }
        except Exception as e:
            self.log_message("Error setting device parameter: " + str(e))
            raise

    def _set_mixer_value(self, track_index, target, value, send_index=None):
        """Set a track's volume, panning, or one send level.

        target: "volume", "panning", or "send" (send_index required for "send").
        """
        try:
            if track_index < 0 or track_index >= len(self._song.tracks):
                raise IndexError("Track index out of range")
            track = self._song.tracks[track_index]
            mixer = track.mixer_device

            if target == "volume":
                param = mixer.volume
            elif target == "panning":
                param = mixer.panning
            elif target == "send":
                if send_index is None:
                    raise Exception("send_index is required when target is 'send'")
                send_index = int(send_index)
                if send_index < 0 or send_index >= len(mixer.sends):
                    raise IndexError("Send index out of range")
                param = mixer.sends[send_index]
            else:
                raise Exception("Unknown target '" + str(target) + "' — expected volume/panning/send")

            old = float(param.value)
            param.value = float(value)
            return {
                "track_index": track_index,
                "target": target,
                "send_index": send_index,
                "old_value": old,
                "value": float(param.value),
                "min": float(param.min),
                "max": float(param.max),
            }
        except Exception as e:
            self.log_message("Error setting mixer value: " + str(e))
            raise

    def get_browser_tree(self, category_type="all"):
        """
        Get a simplified tree of browser categories.
        
        Args:
            category_type: Type of categories to get ('all', 'instruments', 'sounds', etc.)
            
        Returns:
            Dictionary with the browser tree structure
        """
        try:
            # Access the application's browser instance instead of creating a new one
            app = self.application()
            if not app:
                raise RuntimeError("Could not access Live application")
                
            # Check if browser is available
            if not hasattr(app, 'browser') or app.browser is None:
                raise RuntimeError("Browser is not available in the Live application")
            
            # Log available browser attributes to help diagnose issues
            browser_attrs = [attr for attr in dir(app.browser) if not attr.startswith('_')]
            self.log_message("Available browser attributes: {0}".format(browser_attrs))
            
            result = {
                "type": category_type,
                "categories": [],
                "available_categories": browser_attrs
            }
            
            # Helper function to process a browser item and its children
            def process_item(item, depth=0):
                if not item:
                    return None
                
                result = {
                    "name": item.name if hasattr(item, 'name') else "Unknown",
                    "is_folder": hasattr(item, 'children') and bool(item.children),
                    "is_device": hasattr(item, 'is_device') and item.is_device,
                    "is_loadable": hasattr(item, 'is_loadable') and item.is_loadable,
                    "uri": item.uri if hasattr(item, 'uri') else None,
                    "children": []
                }
                
                
                return result
            
            # Process based on category type and available attributes
            if (category_type == "all" or category_type == "instruments") and hasattr(app.browser, 'instruments'):
                try:
                    instruments = process_item(app.browser.instruments)
                    if instruments:
                        instruments["name"] = "Instruments"  # Ensure consistent naming
                        result["categories"].append(instruments)
                except Exception as e:
                    self.log_message("Error processing instruments: {0}".format(str(e)))
            
            if (category_type == "all" or category_type == "sounds") and hasattr(app.browser, 'sounds'):
                try:
                    sounds = process_item(app.browser.sounds)
                    if sounds:
                        sounds["name"] = "Sounds"  # Ensure consistent naming
                        result["categories"].append(sounds)
                except Exception as e:
                    self.log_message("Error processing sounds: {0}".format(str(e)))
            
            if (category_type == "all" or category_type == "drums") and hasattr(app.browser, 'drums'):
                try:
                    drums = process_item(app.browser.drums)
                    if drums:
                        drums["name"] = "Drums"  # Ensure consistent naming
                        result["categories"].append(drums)
                except Exception as e:
                    self.log_message("Error processing drums: {0}".format(str(e)))
            
            if (category_type == "all" or category_type == "audio_effects") and hasattr(app.browser, 'audio_effects'):
                try:
                    audio_effects = process_item(app.browser.audio_effects)
                    if audio_effects:
                        audio_effects["name"] = "Audio Effects"  # Ensure consistent naming
                        result["categories"].append(audio_effects)
                except Exception as e:
                    self.log_message("Error processing audio_effects: {0}".format(str(e)))
            
            if (category_type == "all" or category_type == "midi_effects") and hasattr(app.browser, 'midi_effects'):
                try:
                    midi_effects = process_item(app.browser.midi_effects)
                    if midi_effects:
                        midi_effects["name"] = "MIDI Effects"
                        result["categories"].append(midi_effects)
                except Exception as e:
                    self.log_message("Error processing midi_effects: {0}".format(str(e)))
            
            # Try to process other potentially available categories
            for attr in browser_attrs:
                if attr not in ['instruments', 'sounds', 'drums', 'audio_effects', 'midi_effects'] and \
                   (category_type == "all" or category_type == attr):
                    try:
                        item = getattr(app.browser, attr)
                        if hasattr(item, 'children') or hasattr(item, 'name'):
                            category = process_item(item)
                            if category:
                                category["name"] = attr.capitalize()
                                result["categories"].append(category)
                    except Exception as e:
                        self.log_message("Error processing {0}: {1}".format(attr, str(e)))
            
            self.log_message("Browser tree generated for {0} with {1} root categories".format(
                category_type, len(result['categories'])))
            return result
            
        except Exception as e:
            self.log_message("Error getting browser tree: {0}".format(str(e)))
            self.log_message(traceback.format_exc())
            raise
    
    def get_browser_items_at_path(self, path):
        """
        Get browser items at a specific path.
        
        Args:
            path: Path in the format "category/folder/subfolder"
                 where category is one of: instruments, sounds, drums, audio_effects, midi_effects
                 or any other available browser category
                 
        Returns:
            Dictionary with items at the specified path
        """
        try:
            # Access the application's browser instance instead of creating a new one
            app = self.application()
            if not app:
                raise RuntimeError("Could not access Live application")
                
            # Check if browser is available
            if not hasattr(app, 'browser') or app.browser is None:
                raise RuntimeError("Browser is not available in the Live application")
            
            # Log available browser attributes to help diagnose issues
            browser_attrs = [attr for attr in dir(app.browser) if not attr.startswith('_')]
            self.log_message("Available browser attributes: {0}".format(browser_attrs))
                
            # A raw URI (e.g. "query:Drums#FileId_5332") has no meaningful
            # "/"-separated category structure — route it to the URI-based
            # lookup instead of naively lower()-ing the whole thing as if it
            # were a category name (that always fails: "query:drums#..."
            # matches no category).
            if path.startswith("query:") or "#" in path:
                lookup = self._get_browser_item(path, None)
                if lookup and lookup.get("found") and "item" in lookup:
                    found_item = lookup["item"]
                    return {
                        "path": path,
                        "name": found_item.get("name"),
                        "uri": found_item.get("uri"),
                        "is_folder": found_item.get("is_folder", False),
                        "is_device": found_item.get("is_device", False),
                        "is_loadable": found_item.get("is_loadable", False),
                        "items": [],
                    }
                return {"path": path, "error": "URI not found: " + path, "items": []}

            # Parse the path
            path_parts = path.split("/")
            if not path_parts:
                raise ValueError("Invalid path")

            # Determine the root category
            root_category = path_parts[0].lower()
            current_item = None
            
            # Check standard categories first
            if root_category == "instruments" and hasattr(app.browser, 'instruments'):
                current_item = app.browser.instruments
            elif root_category == "sounds" and hasattr(app.browser, 'sounds'):
                current_item = app.browser.sounds
            elif root_category == "drums" and hasattr(app.browser, 'drums'):
                current_item = app.browser.drums
            elif root_category == "audio_effects" and hasattr(app.browser, 'audio_effects'):
                current_item = app.browser.audio_effects
            elif root_category == "midi_effects" and hasattr(app.browser, 'midi_effects'):
                current_item = app.browser.midi_effects
            else:
                # Try to find the category in other browser attributes
                found = False
                for attr in browser_attrs:
                    if attr.lower() == root_category:
                        try:
                            current_item = getattr(app.browser, attr)
                            found = True
                            break
                        except Exception as e:
                            self.log_message("Error accessing browser attribute {0}: {1}".format(attr, str(e)))
                
                if not found:
                    # If we still haven't found the category, return available categories
                    return {
                        "path": path,
                        "error": "Unknown or unavailable category: {0}".format(root_category),
                        "available_categories": browser_attrs,
                        "items": []
                    }
            
            # Navigate through the path
            for i in range(1, len(path_parts)):
                part = path_parts[i]
                if not part:  # Skip empty parts
                    continue
                
                if not hasattr(current_item, 'children'):
                    return {
                        "path": path,
                        "error": "Item at '{0}' has no children".format('/'.join(path_parts[:i])),
                        "items": []
                    }
                
                found = False
                part_lower = part.lower()
                for child in current_item.children:
                    if not hasattr(child, 'name'):
                        continue
                    child_name_lower = child.name.lower()
                    # Tolerate a missing/mismatched preset extension: "Foo Kit"
                    # should still match "Foo Kit.adg".
                    if (child_name_lower == part_lower or
                            child_name_lower == part_lower + ".adg" or
                            os.path.splitext(child_name_lower)[0] == part_lower):
                        current_item = child
                        found = True
                        break

                if not found:
                    return {
                        "path": path,
                        "error": "Path part '{0}' not found".format(part),
                        "available_children": [c.name for c in current_item.children if hasattr(c, 'name')],
                        "items": []
                    }
            
            # Get items at the current path
            items = []
            if hasattr(current_item, 'children'):
                for child in current_item.children:
                    item_info = {
                        "name": child.name if hasattr(child, 'name') else "Unknown",
                        "is_folder": hasattr(child, 'children') and bool(child.children),
                        "is_device": hasattr(child, 'is_device') and child.is_device,
                        "is_loadable": hasattr(child, 'is_loadable') and child.is_loadable,
                        "uri": child.uri if hasattr(child, 'uri') else None
                    }
                    items.append(item_info)
            
            result = {
                "path": path,
                "name": current_item.name if hasattr(current_item, 'name') else "Unknown",
                "uri": current_item.uri if hasattr(current_item, 'uri') else None,
                "is_folder": hasattr(current_item, 'children') and bool(current_item.children),
                "is_device": hasattr(current_item, 'is_device') and current_item.is_device,
                "is_loadable": hasattr(current_item, 'is_loadable') and current_item.is_loadable,
                "items": items
            }
            
            self.log_message("Retrieved {0} items at path: {1}".format(len(items), path))
            return result
            
        except Exception as e:
            self.log_message("Error getting browser items at path: {0}".format(str(e)))
            self.log_message(traceback.format_exc())
            raise
