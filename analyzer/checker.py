import wave
import os
import struct
import contextlib

def run_integrity_checks(filepath):
    report = {}
    passed = True

    try:
        with contextlib.closing(wave.open(filepath, 'rb')) as wav:
            nchannels = wav.getnchannels()
            sampwidth = wav.getsampwidth()
            framerate = wav.getframerate()
            nframes = wav.getnframes()
            comptype = wav.getcomptype()
            frames = wav.readframes(nframes)

            duration = nframes / float(framerate)
            file_size = os.path.getsize(filepath)

            report['channels'] = nchannels
            report['sample_width'] = sampwidth
            report['frame_rate'] = framerate
            report['nframes'] = nframes
            report['duration'] = round(duration, 2)
            report['compression'] = comptype
            report['file_size_bytes'] = file_size

            # 1. WAV header readable
            report['header_check'] = True

            # 2. Valid sample width
            if sampwidth not in [1, 2, 3, 4]:
                report['invalid_sample_width'] = sampwidth
                passed = False

            # 3. No compression
            if comptype != 'NONE':
                report['compression_error'] = f"Unsupported compression type: {comptype}"
                passed = False

            # 4. Data chunk not empty
            if nframes == 0:
                report['empty_data'] = True
                passed = False

            # 5. Duration and data size consistent
            expected_data_bytes = nframes * sampwidth * nchannels
            if abs(expected_data_bytes - len(frames)) > 1024:  # allow minor difference
                report['data_mismatch'] = f"Expected {expected_data_bytes} bytes, got {len(frames)}"
                passed = False

            # 6. Check for silence (optional threshold)
            silent = all(b == 0 for b in frames[:min(1000, len(frames))])
            report['starts_silent'] = silent
            if silent:
                report['warning'] = 'Begins with silence (could be padding or error)'

            # 7. Check file extension vs header
            if not filepath.lower().endswith('.wav'):
                report['extension_mismatch'] = 'File does not end with .wav'
                passed = False

    except wave.Error as e:
        report['header_check'] = False
        report['error'] = f'Wave error: {str(e)}'
        passed = False
    except Exception as e:
        report['error'] = str(e)
        passed = False

    return passed, report
