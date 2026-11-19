import { describe, expect, it } from 'vitest';
import { PropertiesRegistry, formatKeyValue, parseKeyValue, parseProperties } from '../src/index';

describe('parseKeyValue', () => {
  it('splits lines on the first equals sign and trims keys only', () => {
    expect(parseKeyValue('a=1\n b =x=y\nempty=\n\n=skipped\nflag')).toEqual({ a: '1', b: 'x=y', empty: '', flag: '' });
  });

  it('returns an empty map for null input and lets later duplicates win', () => {
    expect(parseKeyValue(null)).toEqual({});
    expect(parseKeyValue(undefined)).toEqual({});
    expect(parseKeyValue('k=1\nk=2')).toEqual({ k: '2' });
  });

  it('round trips with formatKeyValue', () => {
    const text = formatKeyValue({ name: 'Dude', age: '22' });
    expect(text).toBe('name=Dude\nage=22\n');
    expect(parseKeyValue(text)).toEqual({ name: 'Dude', age: '22' });
    expect(formatKeyValue(new Map([['m', 'v']]))).toBe('m=v\n');
  });
});

describe('parseProperties', () => {
  it('supports every java properties separator and comment style', () => {
    const text = ['# comment', '! also comment', 'a=1', 'b: 2', 'c 3', '  d\t=\t4', 'e', 'url=http://host:9200/path'].join('\n');
    expect(parseProperties(text)).toEqual({ a: '1', b: '2', c: '3', d: '4', e: '', url: 'http://host:9200/path' });
  });

  it('joins continuation lines and decodes escapes', () => {
    const text = 'list=one, \\\n    two, \\\n    three\nescaped\\ key=tab\\there\nunicode=\\u0041\\u00e9\nslash=a\\\\\nnew=line\\nbreak';
    expect(parseProperties(text)).toEqual({
      list: 'one, two, three',
      'escaped key': 'tab\there',
      unicode: 'Aé',
      slash: 'a\\',
      new: 'line\nbreak',
    });
  });

  it('accepts colon separated datadog style keys from the forklift docs', () => {
    const text = 'datadog.lifecycle.counter.pending:false\ndatadog.lifecycle.counter.pending.testQueue:true';
    expect(parseProperties(text)).toEqual({
      'datadog.lifecycle.counter.pending': 'false',
      'datadog.lifecycle.counter.pending.testQueue': 'true',
    });
  });
});

describe('PropertiesRegistry', () => {
  it('stores copies, loads text and removes entries', () => {
    const registry = new PropertiesRegistry();
    const values = { a: '1' };
    registry.set('first', values);
    values.a = 'changed';
    registry.load('second', 'x=y');
    expect(registry.get('first')).toEqual({ a: '1' });
    expect(registry.get('second')).toEqual({ x: 'y' });
    expect(registry.names()).toEqual(['first', 'second']);
    expect(registry.delete('first')).toBe(true);
    expect(registry.get('first')).toBeUndefined();
  });
});
