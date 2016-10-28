import std.stdio;
import std.file;
import std.path;
import std.array;
import std.string;
import std.conv;
import std.parallelism;
import std.concurrency;
import std.process;
import std.uuid;
import std.regex;

int main(string[] args) {

    	//
    	// Startup message
    	//	
    	writeln("PDFSearch v1.0.0");
	
    	//
    	// Looking for the right arguments:
    	//
    	if(args.length < 4) {
    		writeln("Expect three arguments:");
		writeln("(1) a directory where to store the index");
		writeln("(2) a base directory where to start with the search");
		writeln("(3) a command out of this list: 'search', 'index', 'maintain'");
		return 1;
    	}

    	auto dataDirectory = args[1].asNormalizedPath().array;
    	auto pdfDirectory = args[2].asNormalizedPath().array;
    	auto command = args[3].toLower();

    	//
	// Check the external tool dependency:
    	//
    	if(!isAvailableAtPath("pdftotext")) {
		writeln("Please install 'pdftotext' and make it available trough your path.");
		return 2;
    	}

    	//
    	// Check that both directories are available:
    	//
    	if(!pdfDirectory.exists() || !pdfDirectory.isDir()) {
    		writefln("The given PDF directory ('%s') is not available.", pdfDirectory);
		return 3;
    	}

    	if(!dataDirectory.exists()) {
    		writefln("The given data directory ('%s') is not available. Thus, it gets created now.", dataDirectory);
		dataDirectory.mkdirRecurse();
    	} else {
		if(!dataDirectory.isDir()) {
			writefln("The given data directory ('%s') is not a directory.", dataDirectory);
	    		return 4;
		}
    	}

    	//
    	// Check and perhaps set up the environment:
    	//
    	auto lookupFilename = asNormalizedPath(dataDirectory ~ dirSeparator ~ "lookup").array;
    	auto indexDirectory = asNormalizedPath(dataDirectory ~ dirSeparator ~ "data").array;

    	if(!indexDirectory.exists()) {
    		writeln("Create the directory for indexes now.");
		indexDirectory.mkdirRecurse();
    	}

	if(!lookupFilename.exists()) {
		writeln("Create the lookup table now.");
    		std.file.write(lookupFilename, "");
    	}

    	//
    	// Switch regarding the command
    	//
    	writefln("The given command is: '%s'", command);
    	if(command == "index") {
    		index(to!string(pdfDirectory), to!string(lookupFilename), to!string(indexDirectory));
		return 0;
    	}

    	if(command == "search") {
		if(args.length != 8) {
			writeln("For the search command, additional four arguments are needed:");
	    		writeln("(4) a list of necessary keywords");
	    		writeln("(5) a list of forbidden keywords");
	    		writeln("(6) a list of proximity keywords");
	    		writeln("(7) a maximal distance for the proximity keywords");
	    		writeln();
	    		writeln("The necessary keywords must be appear inside the PDF, while the forbidden");
	    		writeln("keywords cannot appear. All proximity keywords must appear at the given");
	    		writeln("maximal distance of words. Arguments can be empty.");
	    		writeln();
	    		writeln("Example:");
	    		writeln(`PDFSearch ~/.pdf ~ search "internet,issue" "" "internet,of,things" 3`);
	    		return 6;
		}

		search(to!string(lookupFilename), to!string(indexDirectory), to!string(pdfDirectory), args[4], args[5], args[6], args[7]);
    		return 0;
    	}

    	if(command == "maintain") {
    		maintain(to!string(lookupFilename), to!string(indexDirectory));
		index(to!string(pdfDirectory), to!string(lookupFilename), to!string(indexDirectory));
		return 0;
    	}

    	writefln("The command '%s' is unknown. Known commands are: 'search', 'index', 'maintain'.", command);
	return 5;
}

// Indexing of PDF files
void index(string pdfDirectory, string lookupFilename, string dataDirectory) {
	scope(exit) {
		writeln("Indexing done.");
    	}

    	writeln("Indexing start.");
    	writeln("Create a list of all PDF files.");
    	auto files = dirEntries(pdfDirectory, "*.pdf", SpanMode.breadth).array;

    	writefln("Found %d PDF files.", files.length);

    	// Read the last known lookup state:
    	auto currentLookup = readText(lookupFilename);

    	// Build the new lookup table, based on the last state:
    	shared(string) lookupString = currentLookup;
    
    	// Define a plotter, who draws the progress bar:
    	auto plotterTid = spawn(function void(size_t maxN, Tid owner) {
    	
		auto maxWidth = 25; // Max count of characters
		auto currentWidth = 0;
		auto currentN = 0;
		auto factor4Width = to!double(maxWidth) / 100.0;

	    	writeln();
		writeln("|0%                   100%|");
		writeln("+-------------------------+");
		std.stdio.write("|");
	    	stdout.flush();

		while(true) {
	    		auto received = receiveOnly!int();
	    		if(received == -1) {
				currentN = to!int(maxN);
	    		} else {
			    	currentN = received;
		    	}

		    	// Calculate the current percentage:
		    	auto currentPercentage = (to!double(currentN) / to!double(maxN)) * 100.0;

			// Calculate the current width:
			auto newWidth = to!int(currentPercentage * factor4Width);

			// No change?
			if(newWidth == currentWidth) {
				continue;
		    	}

			// There was a change. Draw the change:
			std.stdio.write("=".replicate(newWidth - currentWidth));
			stdout.flush();

			// Canceled?
			if(received == -1) {
				break;
			}

			// Store this state:
			currentWidth = newWidth;
		}

		// Append the ending:
		writeln("|");
		writeln();

		// Signal back, that this thread is done:
		owner.send("ACK");

    	}, files.length, thisTid());
    	setMaxMailboxSize(plotterTid, 10000, OnCrowding.ignore);

    	// Define a receiver for all messages of the parallel execution of PDF files:
    	auto receiverTid = spawn(function void(size_t count, shared(string) *lookup, Tid plotter, Tid owner) {
		
		// We know how many messages arrive:
		foreach(number; 0 .. count) {
	    		auto received = receiveOnly!string();
			if(received == "DONE") {
				break;
	    		}
			
			// Show the progress:
			plotter.send(to!int(number));
			if(received == "NEXT") {
		    		continue;
			}

			*lookup = *lookup ~ received ~ "\n";
		}

		owner.send("ACK");
	    
    	}, files.length, &lookupString, plotterTid, thisTid());
    	setMaxMailboxSize(receiverTid, 10000, OnCrowding.block);

    	// Parallel execution of PDF files:
    	foreach(n, filename; parallel(files)) {

		// Only process new files:
		if(currentLookup.indexOf(filename) == -1) {
			
	    		// File is not known yet:
	    		auto dataId = randomUUID().toString();
    			auto pid = spawnProcess(["pdftotext", "-q", filename,  dataDirectory ~ dirSeparator ~ dataId ~ ".txt"]);
			receiverTid.send(dataId ~ ';' ~ filename);
			wait(pid);
	    
		} else {

			// File is known:
	    		receiverTid.send("NEXT");
		}
    	}

    	// Terminate the receivers (in case that not all files were new):
    	receiverTid.send("DONE");
    	plotterTid.send(-1);
    	receiveOnly!string();
    	receiveOnly!string();

    	//
    	// Note: This point gets reached after all PDF files are processed. The foreach gets blocked until all threads are finished.
	//

    	// Write the new lookup table:
    	std.file.write(lookupFilename, lookupString);
}

// Searching for PDF files
void search(string lookupFilename, string dataDirectory, string pdfDirectory, string andKeywords, string notKeywords, string proximityKeywords, string proximityMaxDistanz) {
	scope(exit) {
		writeln("Searching done.");
    	}

    	// Load the lookup table:
    	auto lookupTable = readText(lookupFilename).splitLines();

    	// Reserve space for the maximum of possible matches:
    	shared(string[]) matches; matches.length = lookupTable.length;

    	// Define a matches listener where the results are collected:
    	auto matchesTid = spawn(function void(Tid owner, shared(string[]) results, shared(string) baseDirectory) {
		
		writeln();
		writeln("Please enter the desired number in order to open the file or END to exit.");
		writeln("Matches:");
		size_t counter = -1;
		while(true) {
	    		auto received = receiveOnly!string();
			if(received == "DONE") {
				break;
	    		}

		    	if(received == "READY") {
				writeln("Searching done. Please enter a number to open the file or END to exit.");
		    		continue;
		    	}
			
			counter++;
			writefln("[%5d] %s", counter, received.replace(baseDirectory, ""));
			results[counter] = received;
	    	}

		owner.send("ACK");
	    
    	}, thisTid(), matches, cast(shared) pdfDirectory);
    	setMaxMailboxSize(matchesTid, 10000, OnCrowding.block);

    	// Listener for user's input:
    	auto userTid = spawn(function void(Tid owner, shared(string[]) results, Tid matchesTid) {

		while(true) {
			auto input = readln().strip().toLower();
	    		if(input == "end") {
				matchesTid.send("DONE");
				break;
	    		}

			spawnProcess(["open", "file://" ~ results[to!int(input)]]);
		}

	    	owner.send("ACK");
    	}, thisTid(), matches, matchesTid);

    	// Process in parallel all indexes:
	auto files = dirEntries(dataDirectory, "*.txt", SpanMode.breadth).array;
    	foreach(n, filename; parallel(files)) {
    	
		auto cancel = false;

		// Extract the filename without extension:
		auto uuid = filename.baseName(".txt");
    	
		// Read the entire content:
		auto content = readText(filename).toLower();

		// Get the individual keywords:
		auto andKeywordsElements = andKeywords.toLower().split(",");
		auto notKeywordsElements = notKeywords.toLower().split(",");
		auto proximityKeywordsElements = proximityKeywords.toLower().replace(",", "|");

		// Append the proximity words also to the AND list to ensure, that each word appears in the PDF.
		andKeywordsElements ~= proximityKeywords.toLower().split(",");

		// Check the AND keywords:
		foreach(andKeyword; andKeywordsElements) {
			if(content.indexOf(andKeyword) == -1) {
				// Keyword is not present:
				cancel = true;
				break;
	    		}
		}

		if(!cancel) {

			// Check the NOT keywords:
			foreach(notKeyword; notKeywordsElements) {
				if(content.indexOf(notKeyword) > -1) {
					// Keyword is present:
					cancel = true;
					break;
				}
			}

			if(!cancel) {

				// Proximity search
				auto proximityMatch = false;		

				if(proximityKeywordsElements.length > 0) {
					auto rex = regex(`\b(` ~ proximityKeywordsElements ~ `)(?:\W+\w+){1,` ~ proximityMaxDistanz ~ `}?\W+(` ~ proximityKeywordsElements ~ `)\b`);
					proximityMatch = !matchFirst(content, rex).empty;
				}		

				if(proximityMatch || proximityKeywordsElements.length == 0) {

					// This file is a match! Find the PDF file name:
					foreach(line; lookupTable) {

						if(line.indexOf(uuid) == -1) {
							continue;
						}

						auto cleanLine = line.strip();
						auto elements = cleanLine.split(";");
						auto dataFile = elements[0];
						auto pdfFilename = elements[1];
						matchesTid.send(pdfFilename);
						break;
					}
		    		}
			}
	    	}
    	}

    	// Search is done:
    	matchesTid.send("READY");

    	// Wait for the listeners:
	receiveOnly!string();
	receiveOnly!string();
}

// Maintain the data e.g. delete dead entries, etc.
void maintain(string lookupFilename, string dataDirectory) {
	scope(exit) {
    		writeln("Maintenance done.");
    	}

    	writeln("Maintenance start.");

    	//
    	// 1. Phase
    	//

    	writeln("Phase 1: Checking lookup table.");
    	shared(string) newLookup = "";

    	// Define a receiver for healthy entries:
    	auto healthyReceiverTid = spawn(function void(shared(string) *lookup, Tid owner) {
		
		while(true) {
	    		auto received = receiveOnly!string();
			if(received == "DONE") {
				break;
	    		}
			
			*lookup = *lookup ~ received ~ "\n";
	   	}

		owner.send("ACK");
	    
    	}, &newLookup, thisTid());
    	setMaxMailboxSize(healthyReceiverTid, 10000, OnCrowding.block);
	
	// Read the entire table by lines:
	auto lookupTable = readText(lookupFilename).splitLines();
    	foreach(n, line; parallel(lookupTable)) {
		auto cleanLine = line.strip();
		auto elements = cleanLine.split(";");
		auto dataFile = elements[0];
		auto pdfFilename = elements[1];

		// Is the file existing?
		if(pdfFilename.exists()) {
	    		healthyReceiverTid.send(cleanLine);
		}
    	}

    	// End signal for the receiver:
    	healthyReceiverTid.send("DONE");
	receiveOnly!string(); // Wait for receiver to finish.

    	// Write the maintained lookup table:
    	std.file.write(lookupFilename, newLookup);

    	//
    	// 2. Phase
    	//

    	writeln("Phase 2: Checking indexes.");
    	writeln("Read all file names.");
    	auto files = dirEntries(dataDirectory, "*.txt", SpanMode.breadth).array;
    	foreach(n, filename; parallel(files)) {
    		
		// Extract the filename without extension:
		auto uuid = filename.baseName(".txt");

		// Check if this id is used inside the lookup:
		if(newLookup.indexOf(uuid) == -1) {
	    		// This index is a dead entry: Delete this index.
	    		remove(filename);
		}
    	}
}

// Source: https://p0nce.github.io/d-idioms/#Is-a-specific-program-available-in-PATH?
bool isAvailableAtPath(string executableName)
{
    import std.process: environment;
    import std.path: pathSeparator, buildPath;
    import std.file: exists;
    import std.algorithm: splitter;

    // pathSeparator: Windows uses ";" separator, POSIX uses ":"
    foreach (dir; splitter(environment["PATH"], pathSeparator))
    {
        auto path = buildPath(dir, executableName);
        if (exists(path))
            return true;
    }
    return false;
}
