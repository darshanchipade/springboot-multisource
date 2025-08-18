package com.apple.springboot.model;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.HashMap;

@Data
@EqualsAndHashCode(callSuper = false)
public class Facets extends HashMap<String, Object> {
}
