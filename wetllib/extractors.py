from typing import Union
import socket
import ipaddress


def request_from_dict(request_dict: dict) -> str:
    """

    Args:
        request_dict: dict with such keys as 'WITH', 'SELECT' etc.

    Returns:
        request String for Clickhouse
    """
    request = ""
    sql_tokens = ['WITH', 'SELECT', 'FROM', 'WHERE',
                  'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT', 'FORMAT']
    for ops in sql_tokens:
        try:
            if isinstance(request_dict[ops], list):
                request_dict[ops] = ", ".join(request_dict[ops])
            request += " ".join([ops, str(request_dict[ops]), " "])
        except KeyError:
            continue
    return request


def whois_info(ip: str) -> str:
    from ipwhois import IPWhois, exceptions
    try:
        info = IPWhois(ip).lookup_whois()
    except exceptions.IPDefinedError as e:
        return str(e)
    nets = []
    for net in info['nets']:
        nets.append(", ".join([str(net['cidr']),
                               str(net['name']),
                               str(net['description'])]))
    return ("\n"
            "        Whois info:\n"
            "            - ASN: {asn}\n"
            "            - ASN_CIDR: {asn_cidr},\n"
            "            - country: {asn_ctr},\n"
            "            - description: {desc},\n"
            "            - nets:\n"
            "                {nets}\n"
            "        ").format(asn=str(info['asn']),
                               asn_cidr=str(info['asn_cidr']),
                               asn_ctr=str(info['asn_country_code']),
                               desc=str(info['asn_description']),
                               nets="\n                ".join(nets))


def hostname_lookup(ip: Union[str, ipaddress.IPv4Address]) -> Union[str, None]:
    try:
        name = socket.gethostbyaddr(str(ip))
    except socket.herror:
        return None
    return name[0]


def address_lookup(hostname: str) -> Union[str, ipaddress.IPv4Address, None]:
    try:
        address = socket.gethostbyname(hostname)
    except socket.gaierror:
        return None
    return address
