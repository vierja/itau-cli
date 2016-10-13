import click
import logging

from client import ItauClient


@click.command()
@click.option('--username', help='Itau Link username (usually identity card from Uruguay)')
@click.option('--password', help='Itau Link weak password')
@click.option('--save-csv', is_flag=True, help='Generate a CSV report for each account. (Saved in current directory)')
@click.option('-v', '--verbose', count=True)
def main(username, password, save_csv, verbose):
    if verbose == 0:
        log_level = None
    elif verbose == 1:
        log_level = logging.INFO
    elif verbose > 1:
        log_level = logging.DEBUG

    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('aiohttp.client').setLevel(logging.ERROR)

    if log_level:
        logging.basicConfig(
            format='%(asctime)s : %(levelname)s : %(message)s',
            level=log_level)

    client = ItauClient(username, password)
    if save_csv:
        client.save()
    else:
        from IPython import embed
        embed(display_banner=False)


if __name__ == '__main__':
    main()
