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
uniform vec2 tileRoi;
uniform int showSelection;
uniform int isApproximate;
uniform int dimensions;

uniform sampler2D data;

uniform float dataMin;
uniform float dataMax;

uniform float tile;
uniform float tileHeight;
uniform float selectedBasePair;

#define BORDER_HEIGHT_PX 4.0

vec3 hsv2rgb(vec3 c) {
  vec4 K = vec4(1.0, 2.0 / 3.0, 1.0 / 3.0, 3.0);
  vec3 p = abs(fract(c.xxx + K.xyz) * 6.0 - K.www);
  return c.z * mix(K.xxx, clamp(p - K.xxx, 0.0, 1.0), c.y);
}

void main() {
	float bpPerPixel = (displayedRange.y - displayedRange.x) / windowSize.x;
	float delta = 0.5 / (totalTileRange.y - totalTileRange.x);
	float currBp = mix(currentTileDisplayRange.x, currentTileDisplayRange.y, coord.x);
	float locInTile = mix(tileRoi.x, tileRoi.y, coord.x);

	// load and normalize data value:
	vec4 currData = texture2D(data, vec2(locInTile, 0.0));
	float val = currData.r;
	float chr = currData.g;
	float locInChr = currData.b;
	vec3 chrColor = hsv2rgb(vec3(chr/25.0, 0.7, 1.0));

	vec3 finalColor = vec3(0.0);
	
	if (val == 0.0) {
		finalColor = vec3(250, 192, 94); //  A = yellow
	} else if (val == 0.25) {
		finalColor = vec3(89, 205, 144); // T = green
	} else if (val == 0.5) {
		finalColor = vec3(25, 147, 251); // C = blue
	} else if (val == 0.75) {
		finalColor = vec3(209, 17, 73); // G = red
	}
	
	finalColor *= 1.0/255.0;

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