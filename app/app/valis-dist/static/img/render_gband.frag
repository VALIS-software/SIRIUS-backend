precision mediump float;

varying vec2 coord;

uniform vec3 color1;
uniform vec3 color2;
uniform vec3 color3;
uniform vec3 color4;

uniform vec2 displayedRange; // range of genome currently displayed
uniform vec2 totalRange; // total range of the genome
uniform vec2 currentTileDisplayRange; // range of the tile that is displayed
uniform vec2 totalTileRange; // total range of the current tile

uniform vec2 windowSize;
uniform vec2 selectionBoundsMin;
uniform vec2 selectionBoundsMax;
uniform int showSelection;
uniform int isApproximate;
uniform int dimensions;

uniform sampler2D data;

uniform float dataMin;
uniform float dataMax;

uniform float tile;
uniform float selectedBasePair;
uniform float tileHeight;

#define BORDER_HEIGHT_PX 4.0

vec3 hsv2rgb(vec3 c) {
  vec4 K = vec4(1.0, 2.0 / 3.0, 1.0 / 3.0, 3.0);
  vec3 p = abs(fract(c.xxx + K.xyz) * 6.0 - K.www);
  return c.z * mix(K.xxx, clamp(p - K.xxx, 0.0, 1.0), c.y);
}


void main() {

	float currBp = mix(currentTileDisplayRange.x, currentTileDisplayRange.y, coord.x);
	float locInTile = (currBp - totalTileRange.x) / (totalTileRange.y - totalTileRange.x);
	float bpPerPixel = (displayedRange.y - displayedRange.x) / windowSize.x;
	// load and normalize data value:
	vec4 currData = texture2D(data, vec2(locInTile, 0.0));
	float val = currData.r;
	float chr = currData.g;
	float locInChr = currData.b;
	vec3 chrColor = hsv2rgb(vec3(chr/25.0, 0.7, 1.0));
	
	vec3 finalColor = vec3(val);

	float alpha = 0.0;
	vec3 a;
	vec3 b;
	if (val <= 0.33) {
		alpha = val * 3.0;
		a = color1;
		b = color2;
	} else if (val <= 0.66) {
		alpha = (val - 0.33) * 3.0;
		a = color2;
		b = color3;
	} else {
		alpha = (val - 0.66) * 3.0;
		a = color3;
		b = color4;
	}

	finalColor = mix(a, b, alpha) / 255.0;

	if (val == 0.0) finalColor = vec3(0.0);

	vec3 tintColor = vec3(0.0);
	float pixelsPerBp = windowSize.x/(displayedRange.y - displayedRange.x);
	

	vec2 bMin = vec2(min(selectionBoundsMin.x, selectionBoundsMax.x), min(selectionBoundsMin.y, selectionBoundsMax.y));
	vec2 bMax = vec2(max(selectionBoundsMin.x, selectionBoundsMax.x), max(selectionBoundsMin.y, selectionBoundsMax.y));

	vec2 screenCoord =  gl_FragCoord.xy;
	screenCoord.y = windowSize.y - screenCoord.y;

	vec3 selectionHighlight = vec3(0.0);
	if (showSelection == 1 && screenCoord.x > bMin.x && screenCoord.x < bMax.x) {
		selectionHighlight = vec3(0.3);
	}

	float trackHeightPx = tileHeight * windowSize.y;
	if (coord.y < BORDER_HEIGHT_PX/trackHeightPx) {
		finalColor = chrColor;
	}
	if (coord.y > (1.0 -  BORDER_HEIGHT_PX/trackHeightPx)) {
		finalColor = vec3(locInChr);
	}

	gl_FragColor = vec4(tintColor + finalColor + selectionHighlight, 1.0);	
}