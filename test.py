import os
import pathlib
from main import main
if __name__=="__main__":
    for measurements_file in set([os.path.join("resources/samples/", pathlib.Path(i).stem) for i in os.listdir("resources/samples")]):
        measurements_input = measurements_file+".txt"
        measurements_output = measurements_file+".out"
        output = main(measurements_input)
        expected_output = open(measurements_output).read().strip()
        assert output == expected_output, f"\n{output}\n{expected_output}"