/* forkargs.c
 * Fork for each line of input, limiting parallelism to a specified
 * number of processes.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>
#include <sys/wait.h>
#include <fcntl.h>


/* Spawn off <n> jobs at a time.
 * Read command line arguments from stdin.
 * Use case is:
 *   find -name '*.tar' | forkargs bzip2 -9 
 * Input line is passed as a single argument to the command.
 *
 * We can pass that through to other commands by using sh:
 *
 * find . | forkargs sh -c 'cp $1 dest'
 */

int n_processes = 1;
FILE *trace = NULL;

/* <<< read_line.c */

static char *
read_line_offset (FILE *in,
                  size_t offset)
{
  char buffer[BUFSIZ];
  char *result = NULL;
  if (!feof(stdin)
      && fgets(buffer, BUFSIZ, in))
    {
      size_t len = strlen(buffer);
      if (strstr(buffer, "\n") == NULL)
        {
          /* No newline in this, so get the rest of the string. */
          result = read_line_offset(in, offset + len);
        }
      else
        {
          /* We've read a newline. Allocate a buffer and set the
             terminating end of line character. */
          result = malloc (len + 1 + offset);
          result[len + offset] = '\0';
        }

      /* Copy the buffer into the result. */
      strncpy(&(result[offset]), buffer, len);
    }
  return result;
}


char *
read_line (FILE *in)
{
  return read_line_offset (in, 0);
}

/* read_line.c >>> */

void help()
{
  fprintf (stdout, "Syntax: forkargs -t<out> -j<n>\n");
  fprintf (stdout, " -t<out> trace process control info to <out>\n");
  fprintf (stdout, " -j<n>   Maximum of <n> parallel jobs\n");
}

void bad_arg(char *arg)
{
  fprintf (stderr, "Bad argument: '%s'\n", arg);
  help();
  exit (2);
}

void missing_arg(char *arg)
{
  fprintf (stderr, "Missing parameter to argument: '%s'\n", arg);
  help();
  exit (2);
}

int main (int argc, char *argv[])
{
  char *str;
  int i;
  char **args;
  int first_arg;
  int line_arg;
  int n_active = 0;
  int cpid;

  /* Defaults from environment */
  str = getenv("FORKARGS_J");
  if (str)
    n_processes = atoi (str);

  /* Parse options */
  for (i = 1; i < argc && argv[i][0] == '-'; i++)
    {
      if (argv[i][1] == 'j')
        {
          if (argv[i][2] >= '0' && argv[i][2] <= '9')
            /* '-j<n>' */
            n_processes = atoi(&argv[i][2]);
          else if (argv[i][2])
            /* '-j<garbage>' */
            bad_arg(argv[i]);
          else if (i + 1 < argc)
            /* '-j' '<n>' */
            n_processes = atoi(argv[++i]);
          else
            missing_arg (argv[i]);
        }
      else if (argv[i][1] == 't')
        {
          const char *trace_name = "-";
          if (argv[i][2])
            /* '-t<filename>' */
            trace_name = &argv[i][2];
          else if (i + 1 < argc)
            /* '-t <filename>' */
            trace_name = argv[++i];
          else
            /* '-t' */
            missing_arg (argv[i]);
          if (trace_name[0] == '-' && trace_name[1] == '\0')
            trace = stderr;
          else
            trace = fopen (trace_name, "w");
          if (trace_name && !trace)
            {
              fprintf (stderr, "Cannot open trace file '%s'\n", trace_name);
              exit (0);
            }
        }
      else if (argv[i][1] == 'h')
        {
          help();
          exit (0);
        }
      else
        bad_arg (argv[i]);
    }

  if (n_processes <= 0)
    {
      fprintf (stderr, "Bad process limit (%d)\n", n_processes);
      exit (2);
    }

  first_arg = i;
  args = calloc (argc - first_arg + 2, sizeof (char *));

  for (i = 0; i < argc - first_arg; i++)
    args[i] = argv[i + first_arg];
  line_arg = i;

  while ((str = read_line (stdin)))
    {
      /* Strip newline */
      char *nl = strstr (str, "\n");
      if (nl)
        *nl = '\0';

      /* Construct exec parameters */
      args[line_arg] = str;
      args[line_arg+1] = NULL;

      if (n_active >= n_processes)
        {
          int status;
          if (trace)
            fprintf (trace, "%s: %d processes active, waiting for"
                     " one to finish\n",
                     argv[0], n_active);
          /* Wait for one to exit before proceeding */
          cpid = wait (&status);
          if (cpid == -1)
            {
              perror(argv[0]);
              exit(1);
            }

          if (trace)
            fprintf (trace, "%s: child %d terminated\n", argv[0], cpid);
          n_active--;
        }

      cpid = fork();
      if (cpid)
        {
          /* parent */
          n_active++;
          if (trace)
            fprintf (trace, "%s: started child %d\n", argv[0], cpid);
        }
      else
        {
          /* Child. Execute the process. */
          if (trace)
            {
              fprintf (trace, "%s: exec ", argv[0]);
              for (i = 0; i <= line_arg; i++)
                fprintf (trace, "'%s' ", args[i]);
              fprintf (trace, "\n");
            }
          close(STDIN_FILENO);
          open("/dev/null", O_RDONLY);
          execvp(args[0], args);
          exit(0);
        }

      free (str);
    }

  /* Wait for all children to terminate */
  while (n_active)
    {
      int status;
      if (trace)
        fprintf (trace, "%s: waiting for %d children\n", argv[0], n_active);
      cpid = wait(&status);
      if (trace)
        fprintf (trace, "%s: child %d terminated\n", argv[0], cpid);
      n_active --;
    }
  return 0;
}
