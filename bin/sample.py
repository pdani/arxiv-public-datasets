#!/usr/bin/python3

import argparse
import concurrent.futures
import json
import os.path
import random
import urllib.request


def download(url, path):
    try:
        urllib.request.urlretrieve(url, path)
    except Exception as e:
        print(e)


def getArgs():
    parser = argparse.ArgumentParser(description='Sample arXiv')
    parser.add_argument(
        '--json', required=True, help='Path to the JSON file containing the arXiv metadata.')
    parser.add_argument('--output_dir', required=True,
                        help='Path to the directory where the sampled PDF files and the ids.txt file is written.')
    parser.add_argument('--entry_count', type=int, required=True,
                        help='The number of entries in the JSON file.')
    parser.add_argument('--sample_size', type=int, required=True,
                        help='The number of entries in the resulting sample.')
    return parser.parse_args()


def main():
    args = getArgs()
    indices = set(random.sample(range(args.entry_count), args.sample_size))
    sample = []
    with open(os.path.join(args.output_dir, "ids.txt"), 'w') as output:
        with concurrent.futures.ThreadPoolExecutor(10) as executor:
            futures = []
            for n, line in enumerate(open(args.json)):
                if n not in indices:
                    continue
                sample.append(json.loads(line)['id'])
                output.write(f'{n:09}' + ': ' + sample[-1] + '\n')
                futures.append(executor.submit(download, 'https://arxiv.org/pdf/' +
                                               sample[-1], os.path.join(args.output_dir, f'{n:09}' + '.pdf')))
            concurrent.futures.wait(futures)


if __name__ == "__main__":
    main()
